; Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

(ns jepsen.tikv
  (:require [clojure.string :as str]
            [jepsen
             [cli :as cli]
             [tests :as tests]
             [checker :as checker]
             [generator :as gen]]
            [jepsen.os.debian :as debian]
            [jepsen.mediator.wrapper :as med]
            [jepsen.tikv
             [db :as db]
             [nemesis :as nemesis]
             [register :as register]
             [set :as set]
             [list-append :as list-append]
             [util :as tu]]))

(def bug-pattern
  "An egrep pattern for finding assertion errors in log files."
  "Assertion|assert|panic|fatal|Sanitizer|Fatal")

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {:register       register/workload
   :set            set/workload
   :list-append    list-append/workload})

(def all-workloads
  "A collection of workloads we run by default."
  (remove #{:none} (keys workloads)))

(defn tikv-test
  "Given an options map from the command line runner (e.g.: :nodes, :ssh,
  :concurrency, ...), construct a test map.
  
      :workload    Name of the workload to run"
  [opts]
  (let [workload-name (:workload opts)
        workload ((workloads workload-name) opts)
        nemesis-opts  {:faults (set (:nemesis opts))
                       :partition {:targets [:one :majority :majorities-ring :minority-third]}
                       :pause     {:targets [:one :majority :minority :minority-third]}
                       :kill      {:targets [:one :majority :minority :minority-third]}
                       :interval  (:nemesis-interval opts)}
        nemesis-opts  (assoc nemesis-opts
                             :db              (db/tikv)
                             :nodes           (:nodes opts))
        packages      (nemesis/nemesis-packages nemesis-opts)
        nemesis       (med/adaptive-nemesis packages nemesis-opts)
        gen      (->> (:generator workload)
                      (gen/nemesis (:generator nemesis))
                      (gen/time-limit (:time-limit opts)))
        gen      (if (:final-generator workload)
                   (gen/phases gen
                               (gen/log "Healing cluster")
                               (gen/nemesis (:final-generator nemesis))
                               (gen/log "Waiting for recovery")
                               (gen/sleep (:final-recovery-time opts))
                               (gen/clients (:final-generator workload)))
                   gen)]
    (merge tests/noop-test
           opts
           {:name (str "tikv " (name workload-name))
            :os debian/os
            :db (db/tikv)
            :net med/network
            :pure-generators true
            :client (:client workload)
            :nemesis (:nemesis nemesis)
            :checker (checker/compose
                      {:perf     (checker/perf)
                       :workload (:checker workload)
                       :exceptions (checker/unhandled-exceptions)
                       :assert     (checker/log-file-pattern bug-pattern "kv.stdout")})
            :generator gen})))

(defn parse-nemesis-spec
  "Parses a comma-separated string of nemesis types, and turns it into an
  option map like {:kill-alpha? true ...}"
  [s]
  (if (= s "none")
    {}
    (->> (str/split s #",")
         (map (fn [o] [(keyword o) true]))
         (into {}))))

(def special-nemeses
  "These are the types of failures that the nemesis can perform."
  {:none []
   :all  [:kill
          :pause
          :partition]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(def cli-opts
  "Additional command line options."
  [["-v" "--version VERSION" "The version of TiKV"]
   ["-w" "--workload NAME" "The workload to run"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  100
    :parse-fn tu/parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--nemesis-interval SECONDS"
    "Roughly how long to wait between nemesis operations. Default: 10s."
    :default 3
    :parse-fn tu/parse-long
    :validate [(complement neg?) "should be a non-negative number"]]

   [nil "--nemesis SPEC" "A comma-separated list of nemesis types"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :kill :partition})]]

   [nil "--nemesis-long-recovery" "Every so often, have a long period of no faults, to see whether the cluster recovers."
    :default false
    :assoc-fn (fn [m k v] (update m :nemesis assoc :long-recovery v))]

   [nil "--final-recovery-time SECONDS" "How long to wait for the cluster to stabilize at the end of a test"
    :default 10
    :parse-fn tu/parse-long
    :validate [(complement neg?) "Must be a non-negative number"]]])

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  tikv-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
