(ns scylla.mv
  "Some kind of test for materialized views. Not exactly sure what this does!"
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [client    :as client]
             [generator :as gen]
             [nemesis   :as nemesis]]
            [qbits.alia :as alia]
            [qbits.alia.policy.retry :as retry]
            [qbits.hayt :refer :all]
            [scylla [checker  :as checker]
                    [client   :as c]])
  (:import (clojure.lang ExceptionInfo)
           (java.net InetSocketAddress)))

(defrecord MVMapClient [tbl-created? conn]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/open test node)))

  (setup! [_ test]
    (let [c (:session conn)]
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (c/retry-each
            (alia/execute c (create-keyspace
                              :jepsen_keyspace
                              (if-exists false)
                              (with {:replication {:class :SimpleStrategy
                                                   :replication_factor 3}})))
            (alia/execute c (use-keyspace :jepsen_keyspace))
            (alia/execute c (create-table
                              :map
                              (if-exists false)
                              (column-definitions {:key    :int
                                                   :value    :int
                                                   :primary-key [:key]})
                              (with {:compaction {:class (:compaction-strategy test)}})))
            (try (alia/execute
                   c (str "CREATE MATERIALIZED VIEW mvmap AS SELECT"
                          " * FROM map WHERE value IS NOT NULL"
                          " AND key IS NOT NULL "
                          "PRIMARY KEY (value, key)"
                          "WITH compaction = "
                          "{'class' : '" (:compaction-strategy test)
                          "'};"))
                 (catch com.datastax.driver.core.exceptions.AlreadyExistsException e)))))))

  (invoke! [_ _ op]
    (let [c (:session conn)]
      (c/with-errors op #{:read}
        (alia/execute c (use-keyspace :jepsen_keyspace))
        (case (:f op)
          :assoc (do (alia/execute c
                                   (update :map
                                           (set-columns {:value (:v (:value op))})
                                           (where [[= :key (:k (:value op))]]))
                                   (merge {:consistency :one
                                           :retry-policy (retry/fallthrough-retry-policy)}
                                          (c/write-opts test)))
                     (assoc op :type :ok))
          :read (let [value (->> (alia/execute c
                                               (select :mvmap)
                                               ; TODO: do we really want ALL
                                               ; here?
                                               (merge {:consistency :all}
                                                      (c/read-opts test)))
                                 (#(zipmap (map :key %) (map :value %))))]
                  (assoc op :type :ok :value value))))))

  (close! [_ _]
          (c/close! conn))

  (teardown! [_ _])

  client/Reusable
  (reusable? [_ _] true))

(defn mv-map-client
  "A map implemented using MV"
  ([]
   (->MVMapClient (atom false) nil)))

(defn assocs
  "Generator that emits :assoc operations for sequential integers,
  mapping x to (f x)"
  [f]
  (->> (range)
       (map (fn [x] {:f :assoc :value {:k x, :v (f x)}}))))

(defn workload
  [opts]
  (let [tl (:time-limit opts)]
    {:client    (mv-map-client)
     ; Not exactly sure what this is supposed to do. Looks like it sets x to x
     ; on the map, for sequential integers, then does a read, then turns around
     ; and sets x to -x, and does another read. Is the read timing important?
     ; We should look at associative-map (and maybe move it into this ns if
     ; it's not used elsewhere). There were also no-op replayer conductors
     ; here... what were they for?
     :generator (gen/phases
                  (gen/time-limit (/ tl 2) (assocs identity))
                  {:f :read}
                  (assocs -))
     :final-generator {:f :read}
     :checker (checker/associative-map)}))
