(ns jepsen.mediator.wrapper
  (:require [clojure [core :as c]]
            [clojure.tools.logging :refer [info warn debug]]
            [clojure.data.json :as json]
            [compojure.core :refer [defroutes GET POST]]
            [org.httpkit.client :as http-kit-client]
            [jepsen
             [net :as net]
             [generator :as gen]
             [nemesis :as n]
             [util :as util]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.net.proto :as p]
            [jepsen.store :as store]
            [slingshot.slingshot :refer [try+ throw+]]))

(def control-url "http://127.0.0.1:5000")
(defn control-endpoint [endpoint] (str control-url endpoint))

(defn inform-mediator
  "Lets the mediator know that a new test has started or ended.
   event = :start or :end."
  [test event]
  (let [options {:form-params
                 {:nodes (str (:nodes test))
                  :start_time (:start-time test)
                  :total_count (:test-count test)
                  :exploration_count (:exploration-count test)
                  :store_path (.getCanonicalPath (store/path! test))}}
        url (case event
              :start (control-endpoint "/test/start")
              :end (control-endpoint "/test/end"))
        {:keys [_ error]} @(http-kit-client/post url options)]
    (if error
      (warn "Error contacting the mediator. Error: " error)
      (info "Succesfully established contact with the mediator.")))
  test)

(defn start-mediator-time
  "Lets the mediator know that relative time has started."
  []
  (let [ts (util/origin-time)
        options {:form-params ts}]
    @(http-kit-client/post (control-endpoint "/test/start_time") options)))

(defn about-to-tear-down-db
  "Lets the mediator know that the DB is going to be teared down."
  []
  @(http-kit-client/post (control-endpoint "/test/before_tear_down")))

(defn inform-invoke-op
  "Lets the mediator know that we have invoked an operation."
  [op]
  (let [options {:form-params {:op (pr-str op)}}]
    @(http-kit-client/post (control-endpoint "/client/invoke") options)))

(defn inform-complete-op
  "Lets the mediator know that we have completed an operation."
  [op]
  (let [options {:form-params {:op (pr-str op)}}]
    @(http-kit-client/post (control-endpoint "/client/complete") options)))


(def network
  "Network that interacts with the mediator to introduce faults."
  (reify net/Net
    (drop! [net test src dest]
      (info "[FW] Instructing mediator to drop packets from " src " to " dest)
      @(http-kit-client/post (control-endpoint "/firewall/drop") {:form-params {:src src :dst dest}}))
    (heal! [net test]
      (info "[FW] Instructing mediator to heal the network.")
      @(http-kit-client/post (control-endpoint "/firewall/heal")))
    (slow! [net test])
    (slow! [net test opts])
    (flaky! [net test])
    (fast! [net test])

    p/EnactAtomically
    (enact! [net test grudge]
      ; We'll expand {dst [src1 src2]} into ((src1 dst) (src2 dst) ...)
      (let [pairs
            (mapcat (fn expand [[dst srcs]]
                      (map list srcs (repeat dst)))
                    grudge)
            json (json/write-str pairs)]
        (info "[FW] Instructing mediator to enact partition: " pairs)
        @(http-kit-client/post (control-endpoint "/firewall/enact") {:form-params {:pairs json}})))))

(defn serialize-ctx
  "Clean up ctx for sending to nemesis. If we send it directly,
   it contains things like #object[io.lacuna.bifurcan.Set 0x18212778
   which cannot be parsed by the edn_format Rust crate."
  [ctx]
  (let [ctx' (assoc ctx :free-threads (set (:free-threads ctx)))]
    (pr-str ctx')))

;; The actual choice logic is implemented on the mediator. MediatorGen is a
;; thin wrapper that communicates with the mediator via HTTP to determine
;; which _operation type_ to execute. It then gets the actual operation
;; to execute via the dispatch function passed.
(defrecord MediatorGen [dispatch]
  gen/Generator
  ;; NOTE: `op` should be deterministic, i.e., always return the same op given
  ;; the same `ctx` UNLESS update has been called in the meantime.
  (op [this test ctx]
    (let [options {:form-params {:ctx (serialize-ctx ctx)
                                 :reltime (util/relative-time-nanos)}}
          resp    @(http-kit-client/post (control-endpoint "/nemesis/op") options)]
      (if (:error resp)
        (do (warn "[GEN] Error contacting mediator:" (:error resp))
            ;; We throw an exception to stop the test.
            ;; FIXME: stop gracefully (so the checker runs) rather than abruptly.
            (throw (Exception. "Error contacting mediator! It likely crashed.")))
        (let [op-type (read-string (:body resp)) ;; plain text edn-formatted string
            ;; If we don't need to dispatch the op, we pass it through unmodified.
              dop     (if (:must-dispatch op-type) (or (dispatch op-type test ctx) :cannot-dispatch) op-type)
            ;; Fill in process details for this operation, so it can be executed
              op      (if (or (= dop :pending) (= dop :cannot-dispatch)) :pending (gen/fill-in-op dop ctx))]
          (when-not (= dop :pending)
            (info "[GEN] Received" op-type "from mediator (dispatched to" dop ") =>" op "to nemesis worker."))
          [op this]))))

  (update [this test ctx event]
    (let [options {:form-params {:ctx (serialize-ctx ctx)
                                 :event (pr-str event)
                                 :reltime (util/relative-time-nanos)}}]
      @(http-kit-client/post (control-endpoint "/nemesis/update") options)
      this)))

(defn adaptive-nemesis
  "Constructs an adaptive nemesis based on the given set of nemesis packages.
   `opts` should contain :db and everything necessary to define the generators."
  [pkgs opts]
  (defn get-operations-for-finite-generator
    "Given a FINITE generator, returns a set of operations it might perform."
    [gen]
    (cond
      (nil? gen)
      []

      ;; a single operation
      (map? gen)
      [gen]

      ;; a collection of operations
      (coll? gen)
      (into [] gen)))
  ;; TODO: write a better reflection mechanism, rather than relying on
  ;; the very uninformative Reflection protocol.
  (defn parse-package
    [pkg]
    (let [all-op-types (into [] (n/fs (:nemesis pkg)))
          _ (debug "parsing package with ops" all-op-types)
          enabled? (not (nil? (:generator pkg)))
          ;; hacky: infer which ops are "reset" (resume normal operation) ops
          ;; by looking at the :final-generator
          reset-ops (get-operations-for-finite-generator (:final-generator pkg))
          enabled-ops (if enabled? (:ops pkg) [])
          enabled-op-types (into [] (doall (map :f enabled-ops)))
          parsed {:enabled enabled?
                  :op-types all-op-types
                  :enabled-op-types enabled-op-types
                  :enabled-ops enabled-ops
                  :reset-ops reset-ops}]
      (info "[NEMESIS]" (pr-str parsed))
      parsed))

  (info "[NEMESIS] Constructing adaptive nemesis.")
  (let [nemesis-config {:nemeses (doall (map parse-package pkgs))
                        :opts (dissoc opts :db)}
        options {:form-params {:nemesis (pr-str nemesis-config)}}
        ;; the unmodified nemesis
        nemesis (nc/compose-packages pkgs)]

    ;; let the mediator know what we are doing
    (info "[NEMESIS] ADAPTIVE: " nemesis-config)
    @(http-kit-client/post (control-endpoint "/nemesis/setup") options)

    ;; our nemesis has the same operations as the original, but
    ;; a custom generator
    (assoc nemesis :generator (MediatorGen. (:dispatch nemesis)))))
