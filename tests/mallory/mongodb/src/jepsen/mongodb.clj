(ns jepsen.mongodb
  "Constructs tests and handles CLI arguments"
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [string :as str]
                     [pprint :refer [pprint]]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [tests :as tests]
                    [util :as util :refer [parse-long]]]
            [jepsen.os.debian :as debian]
            [jepsen.generator :as gen]
            [jepsen.mediator.wrapper :as med]
            [jepsen.mongodb [db :as db]
                            [list-append :as list-append]
                            [nemesis :as nemesis]]))

(def bug-pattern
  "An egrep pattern for finding assertion errors in log files."
  "Assertion|assert|Sanitizer|Fatal|fatal")

(def workloads
  {:list-append list-append/workload
   :none        (fn [_] tests/noop-test)})

(def all-workloads
  "A collection of workloads we run by default."
  (remove #{:none} (keys workloads)))

(def workloads-expected-to-pass
  "A collection of workload names which we expect should actually pass."
  (remove #{} all-workloads))

(def all-nemeses
  "Combinations of nemeses for tests"
  [[]
   [:partition]
   [:kill]
   [:pause]
   [:pause :kill :partition :clock :member]])

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :partition]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(def logging-overrides
  "Custom log levels; Mongo's driver is... communicative"
  {"jepsen.mongodb.client"          :error
   "org.mongodb.driver.client"      :error
   "org.mongodb.driver.cluster"     :error
   "org.mongodb.driver.connection"  :error})

(defn mongodb-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (let [workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        db            (db/db opts)
        nemesis-opts  {:faults (set (:nemesis opts))
                       :nodes  (:nodes opts)
                       :partition {:targets [:one :majority :majorities-ring :minority-third]}
                       :pause     {:targets [nil :one :majority :minority :minority-third :all]}
                       :kill      {:targets [nil :one :majority :minority :minority-third :all]}
                       :interval  (:nemesis-interval opts)}
        nemesis-opts  (assoc nemesis-opts
                             :db              db
                             :nodes           (:nodes opts))
        packages      (nemesis/nemesis-packages nemesis-opts)
        nemesis       (med/adaptive-nemesis packages nemesis-opts)]
    (merge tests/noop-test
           opts
           {:name (str "mongodb " (:version opts)
                       " " (name workload-name)
                       (when-let [w (:write-concern opts)] (str " w:" w))
                       (when-let [r (:read-concern opts)] (str " r:" r))
                       (when-let [w (:txn-write-concern opts)] (str " tw:" w))
                       (when-let [r (:txn-read-concern opts)] (str " tr:" r))
                       (when (:singleton-txns opts) " singleton-txns")
                       " " (str/join "," (map name (:nemesis opts))))
            :pure-generators true
            :logging {:overrides logging-overrides}
            :os   debian/os
            :net  med/network
            :db   db
            :checker (checker/compose
                       {:perf       (checker/perf
                                      {:nemeses (:perf nemesis)})
                        :clock      (checker/clock-plot)
                        :stats      (checker/stats)
                        :exceptions (checker/unhandled-exceptions)
                        :workload   (:checker workload)
                        :assert      (checker/log-file-pattern
                                      bug-pattern "mongod.stdout")})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :generator (gen/phases
                         (->> (:generator workload)
                              (gen/stagger (/ (:rate opts)))
                              (gen/nemesis (:generator nemesis))
                              (gen/time-limit (:time-limit opts))))})))

(def cli-opts
  "Additional CLI options"
  [[nil "--hidden NUM" "Number of hidden replicas per replica set."
    :parse-fn parse-long
    :default  0
    :validate [(complement neg?) "Must be non-negative"]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
     :parse-fn parse-nemesis-spec
     :validate [(partial every? #{:pause :kill :partition :clock :member})
                "Faults must be pause, kill, partition, clock, or member, or the special faults all or none."]]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  256
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default 3
    :parse-fn read-string
    :validate [pos? "Must be a positive integer."]]

   [nil "--no-read-only-txn-write-concern" "Don't set write concern on read-only transactions"
    :default false]

   [nil "--postgres-sslmode MODE" "What sslmode should we use to connect to postgres: require, disable?"
    :default "disable"
    :parse-fn str
    :validate [#{"require" "disable"}
               "Should be one of require, or disable"]]

   [nil "--postgres-port NUMBER" "What port should we connect to when talking to postgres?"
    :default 5432
    :parse-fn parse-long]
   
   ["-r" "--rate HZ" "Approximate number of requests per second, total"
    :default 1000
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]

   [nil "--read-concern LEVEL" "What level of read concern to use."
    :default nil]

   [nil "--sharded" "If set, set up a multi-shard MongoDB fronted by Mongos."
    :default false]

   ;[nil "--shard-key KEY" "Either `id` or `value`"
   ; :default :id
   ; :parse-fn keyword
   ; :validate [#{:id :value} "Must be either `id` or `value`"]]

   [nil "--singleton-txns" "If set, execute even single operations in a transactional context."
    :default false]

   [nil "--txn-read-concern LEVEL" "What level of read concern should we use in transactions?"]

   [nil "--txn-write-concern LEVEL" "What level of write concern should we use in transactions?"]

   ["-v" "--version STRING" "What version of MongoDB should we test?"
    :default "4.4.9"]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]

   [nil "--write-concern LEVEL" "What level of write concern to use."
    :default nil]
   ])

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
  (cli/run! (merge (cli/single-test-cmd {:test-fn  mongodb-test
                                         :opt-spec cli-opts})
                   (cli/test-all-cmd {:tests-fn (partial all-tests mongodb-test)
                                      :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
