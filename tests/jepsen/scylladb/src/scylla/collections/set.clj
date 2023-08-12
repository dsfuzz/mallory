(ns scylla.collections.set
  "A set backed by a CQL set."
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]
             [nemesis   :as nemesis]]
            [qbits.alia :as alia]
            [qbits.hayt :refer :all]
            [scylla [client :as c]
                    [db :as db]]))

(defrecord CQLSetClient [tbl-created? conn]
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
                              :sets
                              (if-exists false)
                              (column-definitions {:id    :int
                                                   :elements    (set-type :int)
                                                   :primary-key [:id]})
                              (with {:compaction {:class (:compaction-strategy test)}})))
            )))))

  (invoke! [_ test op]
    (let [s (:session conn)]
      (c/with-errors op #{:read}
        (alia/execute s (use-keyspace :jepsen_keyspace))
        (case (:f op)
          :add (do
                 (alia/execute s
                               (update :sets
                                       (set-columns {:elements [+ #{(:value op)}]})
                                       (where [[= :id 0]]))
                               (merge {:consistency :one}
                                      (c/write-opts test)))
                 (assoc op :type :ok))

          :read (do (db/wait-for-recovery 30 s)
                    (let [value (->> (alia/execute
                                       s
                                       (select :sets
                                               (where [[= :id 0]]))
                                       ; TODO: is ALL really what
                                       ; we want?
                                       (merge {:consistency :all
                                               :retry-policy c/aggressive-read}
                                              (c/read-opts test)))
                                     first
                                     :elements
                                     (into (sorted-set)))]
                      (assoc op :type :ok :value value)))))))

  (close! [_ _]
    (c/close! conn))

  (teardown! [_ _])

  client/Reusable
  (reusable? [_ _] true))

(defn cql-set-client
  "A set implemented using CQL sets"
  []
  (->CQLSetClient (atom false) nil))

(defn workload
  [opts]
  {:client          (cql-set-client)
   :generator       (->> (range)
                         (map (fn [x] {:f :add, :value x})))
   :final-generator (->> {:f :read}
                         gen/repeat
                         (gen/stagger 1)
                         gen/until-ok)
   :checker         (checker/set)})
