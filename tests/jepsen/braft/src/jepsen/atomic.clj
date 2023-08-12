(ns jepsen.atomic
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen
             [cli :as cli]
             [util      :as util :refer [parse-long]]
             [checker   :as checker]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.braft.nemesis :as nemesis]
            [jepsen.braft.db :as db]
            [jepsen.braft.wr :as wr]
            [jepsen.nemesis [combined :as nc]]
            [jepsen.braft.client :as client]))

(def workloads
  "A map of workload names to functions that can take CLI opts and construct
  workloads."
  {:wr-register wr/workload})

(def bug-pattern
  "An egrep pattern for finding errors in log files."
  "Assertion|assert|Segmentation fault|Sanitizer")

(def cas-msg-pattern
  "atomic returns following error for CAS failed"
  (re-pattern "'.*' atomic_cas failed, id: '.*' old: '.*' new: '.*'"))

(def timeout-msg-pattern
  (re-pattern "'.*' atomic_cas timedout, id: '.*' old: '.*' new: '.*'"))

(defn stats-checker
  "A modified version of the stats checker which doesn't care if :crash or
  :debug-topic-partitions ops always crash."
  []
  (let [c (checker/stats)]
    (reify checker/Checker
      (check [this test history opts]
        (let [res (checker/check c test history opts)]
          (if (every? :valid? (vals (dissoc (:by-f res)
                                            :error
                                            :crash)))
            (assoc res :valid? true)
            res))))))

(defn perf-checker
  "A modified perf checker which doesn't render debug-topic-partitions, assign,
  or crash operations."
  [perf-opts]
  (let [c (checker/perf perf-opts)]
    (reify checker/Checker
      (check [this test history opts]
        (checker/check c test
                       (->> history
                            (remove (comp #{:assign
                                            :crash
                                            :error}
                                          :f)))
                       opts)))))

(defn atomic-test
  "Defaults for testing atomic."
  [opts]
  (let [workload-name (:workload opts)
        workload      ((get workloads (:workload opts)) opts)
        nemeses    (nc/nemesis-packages
                    (merge opts
                           {:faults   [:partition :kill :pause :stop]
                            :interval 3}))
        nemesis    (->> nemeses
                        (remove nil?)
                        nc/compose-packages)]
    (merge tests/noop-test
           {:name      "atomic-braft"
            :os        debian/os
            :db        (db/db)
            :client     (:client workload)
            :nemesis      (:nemesis nemesis)
            :generator    (->> (:generator workload)
                               (gen/stagger 1)
                               (gen/nemesis (:generator nemesis))
                               (gen/time-limit (:time-limit opts)))
            :checker (checker/compose
                      {:stats (stats-checker)
                       :clock (checker/clock-plot)
                       :perf (perf-checker
                              {:nemeses (:perf nemesis)})
                       :ex         (checker/unhandled-exceptions)
                       :assert     (checker/log-file-pattern bug-pattern "run.log")
                       :workload   (:checker workload)})
            :perf-opts {:nemeses (:perf nemesis)}
            :ssh {:strict-host-key-checking "false"}}
           opts)))

(def cli-opts
  [["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(defn -main
  "Handles command line arguments and starts the test."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn atomic-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
