(ns jepsen.dqlite
  (:gen-class)
  (:refer-clojure :exclude [test])
  (:require [clojure.java.shell :refer [sh]]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
             [cli :as cli]
             [generator :as gen]
             [store :as store]
             [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.os.container :as container]
            [jepsen.dqlite [db :as db]
             [control :as c]
             [bank :as bank]
             [set :as set]
             [append :as append]
             [tmpfs :as tmpfs]
             [nemesis :as nemesis]]))

(def workloads
  "A map of workload names to functions that can take CLI opts and construct
  workloads."
  {:append append/workload
   :bank   bank/workload
   :set    set/workload})

(def assertion-pattern
  "An egrep pattern for finding assertion errors in log files."
  "Assertion|raft_start|start-stop-daemon|for jepsen|Sanitizer|Fatal")

(defn core-dump-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [blips
            (->> (:nodes test)
                 (pmap (fn [node]
                         (let [{:keys [exit]}
                               (->> (store/path test node "app")
                                    .getCanonicalPath
                                    (sh "test" "-e"))]
                           (when (zero? exit) node))))
                 (remove nil?))]

        {:valid? (empty? blips)
         :count (count blips)
         :blips blips}))))

(defn test
  "Constructs a test from a map of CLI options."
  [opts]
  (let [workload-name (:workload opts)
        workload      ((get workloads (:workload opts)) opts)
        nemesis-opts  {:faults (set (:nemesis opts))
                       :nodes  (:nodes opts)
                       :partition {:targets [:primaries]}
                       :pause     {:targets [nil :one :primaries :majority :all]}
                       :kill      {:targets [nil :one :primaries :majority :all]}
                       :interval  (:nemesis-interval opts)
                       :disk      {:dir     db/data-dir
                                   :size-mb 100}}
        local         (:dummy? (:ssh opts))
        os            (if local container/os debian/os)
        tmpfs         (tmpfs/package nemesis-opts)
        db            (db/db (:db tmpfs))
        nemesis       (nemesis/nemesis-package
                       (assoc nemesis-opts
                              :db              db
                              :nodes           (:nodes opts)
                              :extra-packages  [tmpfs]))]
    (merge tests/noop-test
           opts
           bank/options
           {:name      (str "dqlite-" (name workload-name))
            :pure-generators true
            :members   (atom (into (sorted-set) (:nodes opts)))
            :local     local
            :remote    (if local c/nsenter c/ssh)
            :os        os
            :db        db
            :checker    (checker/compose
                         {:perf        (checker/perf {:nemeses (:perf nemesis)})
                          :clock       (checker/clock-plot)
                          :stats       (checker/stats)
                          :exceptions  (checker/unhandled-exceptions)
                          :assert      (checker/log-file-pattern assertion-pattern "app.log")
                          :core-dump   (core-dump-checker)
                          :workload    (:checker workload)})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :generator (gen/phases
                        (->> (:generator workload)
                             (gen/stagger (/ (:rate opts)))
                             (gen/nemesis (gen/phases
                                           (gen/sleep 5)
                                           (gen/log "Checking cluster stability")
                                           {:type :info, :f :stable, :value nil}
                                           (:generator nemesis)))
                             (gen/time-limit (:time-limit opts)))
                        ;; Allow dust to settle before healing cluster
                        (gen/sleep 3)
                        (gen/log "Healing cluster")
                        (gen/nemesis (:final-generator nemesis))
                        (gen/log "Waiting for recovery")
                        (gen/sleep 2)
                        (gen/log "Checking cluster health and stability")
                        (gen/nemesis {:type :info, :f :health, :value nil})
                        (gen/clients (:final-generator workload)))
            }
           )
    ))

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :kill :partition :stop]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(def cli-opts
  "Command line options for tools.cli"
  [["-v" "--version VERSION" "What version of Dqlite should to install"
    :default "master"]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :kill :stop :disk
                                 :partition :member :clock})
               "Faults must be pause, kill, partition, or member, or the special faults all or none."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default 3
    :parse-fn parse-long
    :validate [pos? "Must be a positive number."]]

   [nil "--latency MSECS" "Expected average one-way network latency between nodes."
    :default 10
    :parse-fn parse-long
    :validate [pos? "Must be a positive number."]]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 10
    :parse-fn parse-long
    :validate [pos? "Must be a positive number."]]

   ["-b" "--binary BINARY" "Use the given pre-built dqlite test application binary."
    :default nil]

   [nil "--cluster-setup-timeout SECS" "How long to wait for the cluster to be ready."
    :default 10
    :parse-fn parse-long
    :validate [pos? "Must be a positive number."]]

   ])

(def single-test-opts
  "CLI options for running a single test"
  [["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :missing (str "--workload " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]])

(def all-nemeses
  "Combinations of nemeses for tests"
  [[]
   [:pause :kill :partition :member]])

(def all-workloads
  "A collection of workloads we run by default."
  (keys workloads))

(def workloads-expected-to-pass
  "A collection of workload names which we expect should actually pass."
  all-workloads)

(defn all-test-options
  "Takes base cli options, a collection of nemeses, workloads, and a test count,
  and constructs a sequence of test options."
  [cli nemeses workloads]
  (for [n nemeses, w workloads, i (range (:test-count cli))]
    (assoc cli
           :nemesis   n
           :workload  w)))

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [test-fn cli]
  (let [nemeses   (if-let [n (:nemesis cli)] [n]  all-nemeses)
        workloads (if-let [w (:workload cli)] [w]
                    (if (:only-workloads-expected-to-pass cli)
                      workloads-expected-to-pass
                      all-workloads))]
    (->> (all-test-options cli nemeses workloads)
         (map test-fn))))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run!
   (merge (cli/serve-cmd)
          (cli/single-test-cmd {:test-fn test
                                :opt-spec (concat cli-opts single-test-opts)})
          (cli/test-all-cmd {:tests-fn (partial all-tests test)
                             :opt-spec cli-opts}))
   args))
