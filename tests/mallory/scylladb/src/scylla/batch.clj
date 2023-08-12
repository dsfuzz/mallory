(ns scylla.batch
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]
             [nemesis   :as nemesis]]
            [scylla [client :as c]]
            [qbits.alia :as alia]
            [qbits.hayt :refer :all]))

(defrecord BatchSetClient [tbl-created? conn]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/open test node)))

  (setup! [_ test]
    (let [s (:session conn)]
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (c/retry-each
            (alia/execute s (create-keyspace
                              :jepsen_keyspace
                              (if-exists false)
                              (with {:replication {:class :SimpleStrategy
                                                   :replication_factor 3}})))
            (alia/execute s (use-keyspace :jepsen_keyspace))
            (alia/execute s (create-table
                              :bat
                              (if-exists false)
                              (column-definitions {:pid    :int
                                                   :cid    :int
                                                   :value  :int
                                                   :primary-key [:pid :cid]})
                              (with {:compaction {:class (:compaction-strategy test)}}))))))))

  (invoke! [this test op]
    (let [s (:session conn)]
      (c/with-errors op #{:read}
        (alia/execute s (use-keyspace :jepsen_keyspace))
        (case (:f op)
          :add (let [value (:value op)]
                 (alia/execute s
                               (str "BEGIN BATCH "
                                    "INSERT INTO bat (pid, cid, value) VALUES ("
                                    value ", 0, " value ");"
                                    "INSERT INTO bat (pid, cid, value) VALUES ("
                                    value ", 1, " value ");"
                                    "APPLY BATCH;")
                               (merge {:consistency :quorum}
                                      (c/write-opts test)))
                 (assoc op :type :ok))

          :read (let [results (alia/execute s
                                            (select :bat)
                                            (merge {:consistency :all}
                                                   (c/read-opts test)))
                      value-a (->> results
                                   (filter (fn [ret] (= (:cid ret) 0)))
                                   (map :value)
                                   (into (sorted-set)))
                      value-b (->> results
                                   (filter (fn [ret] (= (:cid ret) 1)))
                                   (map :value)
                                   (into (sorted-set)))]
                  ; TODO: I don't think this actually verifies anything--the
                  ; failed ops are going to be ignored by the set checker,
                  ; which stops us from detecting the divergence. We can write
                  ; an extra checker to look for these, though!
                  (if-not (= value-a value-b)
                    (assoc op :type :fail :value [value-a value-b])
                    (assoc op :type :ok :value value-a)))))))

  (close! [_ _]
    (c/close! conn))

  (teardown! [_ _])

  client/Reusable
  (reusable? [_ _] true))

(defn batch-set-client
  "A set implemented using batched inserts"
  []
  (->BatchSetClient (atom false) nil))

(defn set-workload
  [opts]
  {:client          (batch-set-client)
   :generator       (->> (range)
                         (map (fn [x] {:type :invoke, :f :add, :value x})))
   :final-generator {:f :read}
   :checker         (checker/set)})
