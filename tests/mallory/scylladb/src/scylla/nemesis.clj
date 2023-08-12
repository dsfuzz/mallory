(ns scylla.nemesis
  "All kinds of failure modes for Scylla!"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.set :as set]
            [scylla [client :as client]
             [db :as sdb]]
            [slingshot.slingshot :refer [try+ throw+]]
            [jepsen [control :as c]
             [db :as db]
             [generator :as gen]
             [nemesis :as n]
             [util :as util :refer [rand-nth-empty
                                    pprint-str]]]
            [jepsen.nemesis [time :as nt]
             [membership :as membership]
             [combined :as nc]]))

; This code for periodically recovering isn't currently used, but might come in
; handy for exploring Scylla behavior going forward.

(defn ordered-soonest-op-map
  "Takes a pair of maps wrapping operations. Each map has the following
  structure:

    :op       An operation
    :weight   An optional integer weighting.

  Returns whichever map has an operation which occurs sooner. If one map is
  nil, the other happens sooner. If one map's op is :pending, the other happens
  sooner. If one op has a lower :time, it happens sooner. If the two ops have
  equal :times, prefers the first op."
  [m1 m2]
  (condp = nil
    m1 m2
    m2 m1
    (let [op1 (:op m1)
          op2 (:op m2)]
      (condp = :pending
        op1 m2
        op2 m1
        (let [t1 (:time op1)
              t2 (:time op2)]
          (if (< t2 t1)
            m2
            m1))))))

(defrecord OrderedAny [gens]
  gen/Generator
  (op [this test ctx]
    (when-let [{:keys [op gen' i]}
               (->> gens
                    (map-indexed
                     (fn [i gen]
                       (when-let [[op gen'] (gen/op gen test ctx)]
                         {:op    op
                          :gen'  gen'
                          :i     i})))
                    (reduce ordered-soonest-op-map nil))]
      [op (OrderedAny. (assoc gens i gen'))]))

  (update [this test ctx event]
    (OrderedAny. (mapv (fn updater [gen]
                         (gen/update gen test ctx event))
                       gens))))

(defn ordered-any
  "Takes multiple generators and binds them together. Operations are taken from
  any generator, preferring earlier over later. Updates are propagated to all
  generators."
  [& gens]
  (condp = (count gens)
    0 nil
    1 (first gens)
    (OrderedAny. (vec gens))))

(defn after-time
  "Adjusts all operations from gen so that they execute no sooner than time t."
  [gen t]
  (gen/map (fn [op] (update op :time max t)) gen))

(defn after-times
  "All ops from gen at start seconds, then start + dt seconds, then start + 2dt
  seconds, etc."
  [start dt gen]
  (->> (iterate (partial + dt) start)
       (map util/secs->nanos)
       (map (partial after-time gen))))

(defn periodically-recover
  "Takes a package and modifies its generator to periodically evaluate
  final-generator."
  [pkg]
  (let [g  (:generator pkg)
        fg (:final-generator pkg)]
    (assoc pkg :generator
           (ordered-any
            (after-times 60 60
                         [(gen/log "Recovering...")
                          fg
                          (gen/sleep 20)
                          (gen/log "Recovery done, back to mischief")])
            g))))

(defn up?
  "Is a given node map up?"
  [node]
  (= :up (:status node)))

(defn merge-views-of-node
  "Merges several views of the same node map."
  [views]
  (assoc (reduce merge views)
         :status (first (sort-by {:up 2
                                  :down 1}
                                 (map :status views)))
         :state  (first (sort-by {:normal 1
                                  :moving 2
                                  :leaving 3
                                  :joining 4}
                                 (map :state views)))))

(defn add-op
  "Generates an add-node op for a membership state, if possible. The only nodes
  we can add are those flagged as `free`."
  [state]
  (when-let [node (rand-nth-empty (seq (:free state)))]
    {:type :info, :f :add-node, :value node}))

(def max-removed-nodes
  "Try not to remove/wipe more than this many nodes at once."
  2)

(defn removed-or-free-nodes
  "Returns the set of nodes which are known to be free or are in the process of
  being removed or decommissioned."
  [state]
  (clojure.set/union (:free state)
                     (->> (:pending state)
                          (map first)
                          (filter (comp #{:remove-node} :f))
                          (map (comp :node :node :value))
                          set)
                     (->> (:pending state)
                          (map first)
                          (filter (comp #{:decommission-node} :f))
                          (map :value)
                          set)))

(defn up-nodes
  "Takes a state, and yields a collection of nodes which are part of the
  cluster, and think they're up."
  [state]
  (for [[via node-view] (:node-views state)
        node            node-view
        :when (and (= via (:node node))
                   (up? node)
                   (not (contains? (:free state) via)))]
    via))

(defn repair-op
  "Generates a repair op for a membership state, if possible."
  [state]
  (when-let [n (rand-nth-empty (up-nodes state))]
    {:type :info, :f :repair-node, :value n}))

(defn remove-op
  "Generates a remove node op for a membership state, if possible."
  [state]
  ;(info "state" (pprint-str state))
  ;(info "nv" (pprint-str (:node-views state)))
  (when-let [v (rand-nth-empty
                (for [[via node-view] (:node-views state)
                      node            node-view]
                  (when ; We can only remove nodes `via` thinks are down
                   (and (not (up? node))
                          ; And not nodes which are free. I'm not sure
                          ; we actually want to prevent this, but I
                          ; *think* it steers us into
                          ; more-likely-to-succeed territory.
                        (not (contains? (:free state) (:node node)))
                          ; Don't try to remove self.
                        (not= via (:node node))
                          ; Don't remove if we've got too many nodes freed or
                          ; being removed at once.
                        (< (count (removed-or-free-nodes state))
                           max-removed-nodes))
                    {:via via
                     :node (select-keys node [:node :id])})))]
    {:type :info, :f :remove-node, :value v}))

(defn decommission-op
  "Generates a decommission node op for a membership state, if possible."
  [state]
  ; Don't remove too many nodes
  (when (< (count (removed-or-free-nodes state))
           max-removed-nodes)
    (when-let [n (rand-nth-empty (up-nodes state))]
      {:type  :info
       :f     :decommission-node
       :value n})))

(defn wipe-op
  "Generates a wipe op for a membership state, if possible. We can issue a wipe
  for any node which has a remove or decommission op pending. The idea is that
  sometimes this will be politely sequenced after the remove/decom, and other
  times, we'll nuke a node while it's only partway removed, but we'll generally
  avoid nuking healthy nodes."
  [state]
  (let [pending (map first (:pending state))
        nodes (concat (->> pending
                           (filter (comp #{:remove-node} :f))
                           (map (comp :node :node :value)))
                      (->> pending
                           (filter (comp #{:decommission-node} :f))
                           (map :value)))]
    (when-let [node (rand-nth-empty nodes)]
      {:type :info, :f :wipe-node, :value node})))

(defmacro with-nodetool-errors
  "Evals body, converting nodetool errors to values like :conn-refused."
  [& body]
  `(try+ ~@body
         (catch [:type :jepsen.control/nonzero-exit] e#
           (condp re-find (:out e#)
             #"Connection refused"         :conn-refused
             #"alive and owns this ID"     :node-considered-alive
             #"removenode is in progress"  :remove-in-progress
             #"Host ID not found"          :host-id-not-found
             #"failed to repair \d+ sub ranges" [:failed-to-repair-sub-ranges
                                                 (:out e#)]
             #"Repair job has failed"      :repair-failed
             (throw+ e#)))))

; `free` is a set of nodes we've removed from the cluster and destroyed data
; on.
(defrecord MembershipState [db node-views view pending free faults]
  membership/State
  (node-view [this test node]
    ; TODO: something is weird here. I think nodetool status might only return
    ; lines for SOME but not all of the cluster? Maybe? Maybe our parser is
    ; broken.
    (try+ (map (fn [n] (select-keys n [:status :state :node :id]))
               (sdb/nodetool-status test))
          (catch [:exit 1] e
            ;(info e)
            [{:node node, :status :down}])
          (catch [:exit 2] e
            ;(info e)
            [{:node node, :status :down}])
          (catch [:exit 137] e
            ; Killed
            [{:node node, :status :down}])))

  (merge-views [this test]
    ; TODO: something more clever here, like preferring node's own view of
    ; themselves? Preferring up over down? We don't actually *use* the merged
    ; view that much, because we're hunting for subjectively down nodes for
    ; removal, so this isn't super critical yet.
    (->> node-views
         (mapcat val)
         (group-by :node)
         vals
         ; Pick any representation of a given node.
         (map merge-views-of-node)
         (sort-by :node)))

  (fs [this]
    #{:add-node :remove-node :decommission-node :wipe-node
      :repair-node :pass})

  (op [this test]
    (or (->> (concat (when (faults :remove)
                       [(remove-op this) (wipe-op this) (add-op this)])
                     (when (faults :decommission)
                       [(decommission-op this) (wipe-op this) (add-op this)])
                     (when (faults :repair)
                       [(repair-op this)]))
             (remove nil?)
             rand-nth-empty)
        ; Well this is awkward. We're wrapped in a gen/mix along with the
        ; kill, pause, and partition generators. We need them to run first so
        ; we can start removing nodes. But if we return :pending, we'll be
        ; *stuck* here indefinitely: gen/mix won't move onto anyone else,
        ; because it's deterministic. Later I'm gonna go patch that, but for
        ; now I'm on the clock, and these changes to jepsen's core are
        ; already extensive enough...
        {:type :info, :f :pass}))

  (invoke! [this test {:keys [f value] :as op}]
    (assoc
     op :value
     (case f
       :pass :passed

       :add-node
       (do (c/on-nodes test [value]
                       (fn [test node]
                         (sdb/enable!)
                         (db/start! db test node)))
           [:added value])

       :remove-node
       (with-nodetool-errors
         (sdb/remove-node! test (:via value) (:node value)))

       :decommission-node
       (with-nodetool-errors (sdb/decommission-node! test value))

       :wipe-node
       (do (c/on-nodes test [value]
                       (fn [test node]
                         (sdb/wipe! db test node)
                         (sdb/disable!)))
           [:wiped value])


       :repair-node
       (with-nodetool-errors (sdb/repair-node! test value)))))

  (resolve [this test]
    this)

  (resolve-op [this test [op op']]
    (case (:f op)
      ; Trivial
      :pass this

      ; When we add a node, it's no longer free.
      :add-node
      (update this :free disj (:value op))

      ; We're done removing a node once it's free, or if we know the remove
      ; definitely failed.
      :remove-node
      (when (or (#{:conn-refused
                   :host-id-not-found
                   :node-considered-alive
                   :remove-in-progress}
                 (:value op'))
                (free (:node (:node (:value op)))))
        this)

      ; We're done decommissioning a node once it's free, or if we know the
      ; remove definitely failed.
      :decommission-node
      (when (or (#{:conn-refused
                   :host-id-not-found
                   :node-considered-alive}
                 (:value op'))
                (free (:value op)))
        this)

      ; Once wiped, we can mark this node as free.
      :wipe-node
      (update this :free conj (:value op))

      ; Repairs are immediately resolved.
      :repair-node
      this)))

(defn membership-package
  "Constructs a membership nemesis package if (:faults opts) includes :members"
  [opts]
  (let [opts (if (some #{:remove :decommission :repair} (:faults opts))
               (update opts :faults conj :membership)
               opts)]
    (when-let [pkg (-> opts
                       (assoc :membership {:state (map->MembershipState
                                                   {:db   (:db opts)
                                                    :free #{}
                                                    :faults (:faults opts)})
                                           :log-resolve-op? false
                                           :log-resolve?    true
                                           :log-node-views? false
                                           :log-view?       false})
                       membership/package)]
      (defn dispatch [op test ctx]
        (let [state (deref (:state (:nemesis pkg)))]
          (case (:f op)
            :add-node ((fn add-node [_ _] (add-op state)) test ctx)
            :remove-node ((fn remove-node [_ _] (remove-op state)) test ctx)
            :decommission-node ((fn decommission-node [_ _] (decommission-op state)) test ctx)
            :repair-node ((fn repair-node [_ _] (repair-op state)) test ctx))))

      ; At the end of the test, re-add everyone.
      (assoc pkg
             :final-generator
             (fn final-gen [test ctx]
               (info :nemesis (-> pkg :nemesis))
               (when-let [node (->> pkg :nemesis :state deref
                                    removed-or-free-nodes first)]
                 {:type :info, :f :add-node, :value node}))
             :perf #{{:name "membership"
                      :fs   #{:add-node
                              :repair-node
                              :decommission-node
                              :remove-node}
                      :color "#278B66"}}
             :ops [{:f :add-node :values [nil]}
                   {:f :remove-node :values [nil]}
                   {:f :decommission-node :values [nil]}
                   {:f :repair-node :values [nil]}]
             :dispatch dispatch))))

(defn nemesis-packages
  "Constructs a nemesis and generators for dqlite."
  [opts]
  (let [opts (update opts :faults set)]
    (->> (concat [(nc/partition-package opts)
                  (nc/db-package opts)]
                 (:extra-packages opts))
         (remove nil?))))

(defn package
  "Constructs a {:nemesis, :generator, :final-generator} map for the test.
  Options:

      :interval How long to wait between faults
      :db       The database we're going to manipulate.
      :faults   A set of faults, e.g. #{:kill, :pause, :partition}
      :targets  A map of options for each type of fault, e.g.
                {:partition {:targets [:majorities-ring ...]}}"
  [opts]
  (let [membership (membership-package opts)
        pkg (->> (nc/nemesis-packages opts)
                 (concat [membership])
                 (remove nil?)
                 nc/compose-packages
                 ;periodically-recover
                 )]
    ; Just for testing membership generator behavior--we create a partition to
    ; get things started, then let it remove/wipe, then rejoin.
    ;(assoc pkg :generator
    ;       [(gen/once (fn [test ctx]
    ;                    {:type :info
    ;                     :f :start-partition
    ;                     :value (n/complete-grudge (split-at 1 (:nodes test)))}))
    ;        (gen/limit 5 (:generator membership))
    ;        (gen/once {:type :info, :f :stop-partition})
    ;        (:generator membership)])

    pkg))
