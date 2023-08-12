(ns scylla.list-append
  "This test performs transactional appends and reads of various keys--each a
  distinct row containing a single CQL list value."
  (:refer-clojure :exclude [read])
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.tests.cycle.append :as append]
            [scylla [client :as c]]
            [qbits [alia :as a]
                   [hayt :as h]]))

(defn table-for
  "What table should we use for this key?"
  [test k]
  (str "lists"))

(defn all-tables
  "All tables for a test."
  [test]
  (mapv (partial table-for test) [0]))

(defn mop-query
  "Takes a test and an [f k v] micro-op. Generates a query for this micro-op,
  suitable for inclusion in a batch transaction."
  [test [f k v]]
  (case f
    :append (merge (h/update (table-for test k)
                      (h/set-columns {:value [+ [v]]})
                      (h/where [[= :part 0]
                                [= :id k]]))
                   (when (:lwt test)
                     ; This trivial IF always returns true.
                     (h/only-if [[= :lwt_trivial nil]])))
    ; Dunno how to read. UPDATE's won't return values that aren't in the IF
    ; clause, and if we use IF on the `value` column, we need it to somehow
    ; *always* succeed. If CQL allowed OR (instead of just AND), that'd be
    ; great. Maybe there's some way to ask... not-equal? `value IS NOT [-1]`
    ; explodes--I guess you can't express a negative in CQL? You also can't say
    ; CONTAINS, which rules out having a placeholder element of some kind...
    ;:r (h/update (table-for test k)
    ;             (h/set-columns {:lwt_trivial nil})
    ;             (h/where [[= :part 0]
    ;                       [= :id k]])
    ;             (h/only-if [[h/contains :value -1]]))
    ;
    ; One option for batch reads might be to do an initial read at, say, quorum
    ; or one, then to *confirm* that read using a CAS? But of course if the CAS
    ; *failed*, we'd break blind writes too, and that might be *worse* than not
    ; reading at all.
    ))

(defn apply-batch!
  "Takes a test, a session, and a txn. Performs the txn in a batch,
  returning the resulting txn."
  [test session txn]
  (let [queries (map (partial mop-query test) txn)
        ; _ (info :query (h/->raw (h/batch (apply h/queries queries))))
        results (->> (a/execute session
                                (h/batch (apply h/queries queries))
                                (c/write-opts test))
                     c/assert-applied
                     ; We get back a collection of rows *out* of order. Also,
                     ; due to a bug (sigh) we'll sometimes be missing rows. But
                     ; once that's fixed, we should be able to map based on the
                     ; `id` columns to their prior states, which we use to read
                     ; data.
                     (map (juxt :id identity))
                     (into {}))]
    ; (info :results results)
    (mapv (fn [[f k v :as mop]]
            (let [res (get results k)]
              (info :res [f k v] (pr-str res))
              (case f
                :r      [f k (:value res)]
                :append mop)))
            txn)))

(defn single-read
  "Takes a test, session, and a transaction with a single read mop. performs a
  single CQL select by primary key, and returns the completed txn."
  [test session [[f k v]]]
  [[f k (->> (a/execute session
                        (h/select (table-for test k)
                                  (h/where [[= :part 0]
                                            [= :id   k]]))
                        (merge {:consistency :serial}
                               (c/read-opts test)))
             first
             :value)]])

(defn single-append!
  "Takes a test, session, and a transaction with a single append mop. Performs
  the append via a CQL conditional update."
  [test session txn]
  (let [[f k v] (first txn)]
    (c/assert-applied
      (a/execute session
                 (merge (h/update (table-for test k)
                           (h/set-columns {:value [+ [v]]})
                           (h/where [[= :part 0]
                                     [= :id k]]))
                        (when (:lwt test)
                          (h/only-if [[= :lwt_trivial nil]])))
                 (c/write-opts test))))
  txn)

(defn append-only?
  "Is this txn append-only?"
  [txn]
  (every? (comp #{:append} first) txn))

(defn read-only?
  "Is this txn read-only?"
  [txn]
  (every? (comp #{:r} first) txn))

(defn apply-txn!
  "Takes a test, a session, and a txn. Performs the txn, returning the
  completed txn."
  [test session txn]
  (if (= 1 (count txn))
    (cond (read-only?   txn) (single-read     test session txn)
          (append-only? txn) (single-append!  test session txn)
          true               (assert false "what even is this"))
    (apply-batch! test session txn)))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open test node)))

  (setup! [this test]
    (let [s (:session conn)]
      (c/retry-each
        (a/execute s (h/create-keyspace
                       :jepsen_keyspace
                       (h/if-exists false)
                       (h/with {:replication {:class :SimpleStrategy
                                              :replication_factor 3}})))
        (a/execute s (h/use-keyspace :jepsen_keyspace))
        (doseq [t (all-tables test)]
          (a/execute s (h/create-table
                         t
                         (h/if-exists false)
                         (h/column-definitions {:part         :int
                                                :id           :int
                                                ; We can't do LWT without SOME
                                                ; kind of IF statement (why?),
                                                ; so we leave a trivial null
                                                ; column here.
                                                :lwt_trivial    :int
                                                :value        (h/list-type :int)
                                                :primary-key  [:part :id]})
                         (h/with {:compaction {:class (:compaction-strategy test)}})))))))

  (invoke! [this test op]
    (let [s (:session conn)]
      (c/with-errors op #{}
        (a/execute s (h/use-keyspace :jepsen_keyspace))
        (assoc op
               :type  :ok
               :value (apply-txn! test s (:value op))))))

  (close! [this test]
    (c/close! conn))

  (teardown! [this test])

  client/Reusable
  (reusable? [_ _] true))

(defn workload
  "See options for jepsen.tests.append/test"
  [opts]
  (let [opts (assoc opts :consistency-models
                    (if (and (:lwt opts)
                             (= :serial (:read-consistency opts :serial)))
                      ; Under LWT updates and SERIAL reads, we should
                      ; get strict serializability.
                      [:strict-serializable]
                      ; Otherwise, the Scylla docs claim that UPDATE and BATCH
                      ; are "performed in isolation" when on a single partition
                      ; key. That's the case for our workload, so we search for
                      ; serializability.
                      [:serializable]))
        w (append/test opts)]
    (assoc w
           :client (Client. nil)
           :generator (gen/filter (fn [op]
                                    (let [txn (:value op)]
                                      ; We can't do SELECT IN due to a
                                      ; limitation in Scylla CQL. :(
                                      (or (= 1 (count txn))
                                          (append-only? txn))))
                                  (:generator w))
           )))
