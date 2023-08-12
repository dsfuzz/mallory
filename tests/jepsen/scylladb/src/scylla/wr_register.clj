(ns scylla.wr-register
  "This test performs transactional writes and reads to a set of registers, each stored in a distinct row containing a single int value."
  (:refer-clojure :exclude [read])
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.tests.cycle.wr :as wr]
            [scylla [client :as c]]
            [qbits [alia :as a]
                   [hayt :as h]]))

(defn table-count
  "How many tables should we use for this test?"
  [test]
  (:table-count test))

(defn table-for
  "What table should we use for this key?"
  [test k]
  (str "registers_"
       (mod (hash [:table k]) (table-count test))))

(defn all-tables
  "All tables for a test."
  [test]
  (mapv (partial str "registers_")
        (range (table-count test))))

(defn part-count
  "How many partitions should we use for this test?"
  [test]
  (:partition-count test))

(defn part-for
  "What partition should we use for this key?"
  [test k]
  (mod (hash [:part k]) (part-count test)))

(defn maybe-long
  "Coerces non-null values to longs; nil values remain nil."
  [x]
  (when x (long x)))

(defn write-batch!
  "Takes a test, a session, and a write-only txn. Performs the txn in a batch,
  batch, returning the resulting txn."
  [test session txn]
  (let [queries (map (fn [[f k v]]
                       (merge (h/update (table-for test k)
                                        (h/set-columns {:value v})
                                        (h/where [[= :part (part-for test k)]
                                                  [= :id k]]))
                              (when (:lwt test)
                                ; This trivial IF always returns true.
                                (h/only-if [[= :lwt_trivial nil]]))))
                     txn)
        ; _ (info :queries queries)
        results (a/execute session (h/batch (apply h/queries queries))
                           (c/write-opts test))]
    (c/assert-applied results)
    ; Batch results make no sense so we... just ignore them. Wooo!)
    txn))

(defn read-many
  "Takes a test, a session, and a read-only txn. Performs the read as a single
  CQL select, returning the resulting txn."
  [test session txn]
  (let [ks      (distinct (map second txn))
        tables  (distinct (map (partial table-for test) ks))
        _       (assert (= 1 (count tables)))
        table   (first tables)
        parts   (distinct (map (partial part-for test) ks))
        _       (assert (= 1 (count parts)))
        part    (first parts)
        results (a/execute session
                           (h/select table
                                     (h/where [[=   :part part]
                                               [:in :id   ks]]))
                           (merge {:consistency :serial}
                                  (c/read-opts test)))
        ; We can stitch these back together because ids are globally unique.
        values  (into {} (map (juxt :id (comp maybe-long :value)) results))]
    (mapv (fn [[f k v]] [f k (get values k)]) txn)))

(defn single-write!
  "Takes a test, session, and a transaction with a single write mop. Performs
  the write via a CQL conditional update."
  [test session txn]
  (let [[f k v] (first txn)]
    (c/assert-applied
      (a/execute session
                 (merge (h/update (table-for test k)
                                  (h/set-columns {:value v})
                                  (h/where [[= :part (part-for test k)]
                                            [= :id k]]))
                        (when (:lwt test)
                          (h/only-if [[= :lwt_trivial nil]])))
                 (c/write-opts test))))
  txn)

(defn single-read
  "Takes a test, session, and a transaction with a single read mop. performs a
  single CQL select by primary key, and returns the completed txn."
  [test session [[f k v]]]
  [[f k (->> (a/execute session
                        (h/select (table-for test k)
                                  (h/where [[= :part (part-for test k)]
                                            [= :id   k]]))
                        (merge {:consistency :serial}
                               (c/read-opts test)))
             first
             :value
             maybe-long)]])

(defn write-only?
  "Is this txn write-only?"
  [txn]
  (every? (comp #{:w} first) txn))

(defn read-only?
  "Is this txn read-only?"
  [txn]
  (every? (comp #{:r} first) txn))

(defn single-part?
  "Are all keys in this txn located in a single partition?"
  [test txn]
  (<= (count (set (map (comp (partial part-for test) second) txn))) 1))

(defn single-table?
  "Are all keys in this txn located in a single table?"
  [test txn]
  (<= (count (set (map (comp (partial table-for test) second) txn))) 1))

(defn apply-txn!
  "Takes a test, a session, and a txn. Performs the txn, returning the
  completed txn."
  [test session txn]
  (if (= 1 (count txn))
    (cond (read-only?  txn) (single-read   test session txn)
          (write-only? txn) (single-write! test session txn)
          true              (assert false "what even is this"))
    (cond (read-only? txn)  (read-many     test session txn)
          (write-only? txn) (write-batch!  test session txn)
          true              (assert false "what even is this"))))

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
                                                :lwt_trivial  :int
                                                :value        :int
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

; This version of the client stores every key in a single row's CQL map, and
; does *not* use transactional updates. TODO: make this usable either with or
; without transactional writes, and extract into a shared namespace where this
; test (which should use txns) and the write-isolation test (which shouldn't
; use txns) can share it.
(defrecord SingleRowClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open test node)))

  (setup! [_ test]
    (let [s (:session conn)]
      (c/retry-each
        (a/execute s (h/create-keyspace
                       :jepsen_keyspace
                       (h/if-exists false)
                       (h/with {:replication {:class :SimpleStrategy
                                              :replication_factor 3}})))
        (a/execute s (h/use-keyspace :jepsen_keyspace))
        (a/execute s (h/create-table
                       :maps
                       (h/if-exists false)
                       (h/column-definitions {:id           :int
                                              ; We can't do LWT without SOME
                                              ; kind of IF statement (why?),
                                              ; so we leave a trivial null
                                              ; column here.
                                              :lwt_trivial  :int
                                              :value        (h/map-type
                                                              :int :int)
                                              :primary-key  :id})
                       (h/with {:compaction {:class (:compaction-strategy test)}}))))))

  (invoke! [_ test op]
    (let [s (:session conn)
          txn (:value op)]
      (c/with-errors op #{}
        (a/execute s (h/use-keyspace :jepsen_keyspace))
        (cond (read-only? txn)
              (let [res (->> (a/execute s (h/select :maps
                                                    (h/where [[= :id 0]]))
                                        (c/read-opts test))
                             first
                             :value)
                    ; Bind that map of keys to values back into the txn
                    txn' (mapv (fn [[f k v]]
                                 [f k (get res k)])
                               txn)]
                (assoc op :type :ok, :value txn'))

              (write-only? txn)
              ; Compute a map of keys to final values resulting from each write
              (let [effects (->> txn
                                 (map (comp vec rest))
                                 (into {}))]
                (a/execute s (h/update
                               :maps
                               (h/where [[= :id 0]])
                               (h/set-columns {:value [+ effects]}))
                           (c/write-opts test))
                (assoc op :type :ok))

              true (assert false "can't run mixed read/write txns")))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn))

  client/Reusable
  (reusable? [_ _] true))

(defn generator
  "This is sort of silly, but... we want to test a mix of single-key and
  multi-key txns. However, Scylla imposes a bunch of restrictions on those
  txns: they have to be on a single partition, a single table, can't mix reads
  and writes, etc. If we naively filter the usual Elle txn generator, we wind
  up throwing out almost *every* multi-key txn.

  To work around this (and it's not a GREAT workaround, mind you), we actually
  construct a generator with a mandatory longer txn length, filter that, then
  cut its txns down to size randomly."
  [opts]
  (let [lower (:min-txn-length opts 1)
        upper (:max-txn-length opts)
        delta (- upper lower)]
    (if (<= upper 1)
      ; We're only generating singleton txns
      (wr/gen opts)
      ; We want a mix of singleton and longer txns
      (->> (assoc opts :min-txn-length 2)
           wr/gen
           (gen/filter (fn [op]
                         (let [txn (:value op)]
                           ; We can't do mixed rw txns, nor can we execute
                           ; txns across multiple partitions or tables.
                           (and (single-table? opts txn)
                                (single-part? opts txn)
                                (or (read-only? txn)
                                    (write-only? txn))))))
           ; Cut txns down to size if needed. Yeah, this re-biases the
           ; distribution of lengths a bit, but... it's not awful.
           (gen/map (fn [op]
                      (let [size (+ lower (rand-int delta))]
                        (assoc op :value (vec (take size (:value op)))))))))))

(defn workload
  "See options for jepsen.tests.append/test"
  [opts]
  {:client    (Client. nil)
   :generator (generator opts)
   :checker   (wr/checker
                (merge
                  (if (and (:lwt opts)
                           (= :serial (:read-consistency opts :serial)))
                    ; If all updates use LWT and all reads use SERIAL, we
                    ; expect strict-1SR.
                    {:linearizable-keys? true
                     :consistency-models [:strict-serializable]}

                    ; Otherwise, Scylla docs claim UPDATE and BATCH are
                    ; "performed in isolation" on single partitions; we
                    ; should observe serializability--but we can't rely on
                    ; sequential or linearizable key constraints.
                    {:consistency-models [:serializable]})
                  opts))})

; Patch a bug in Elle real quick--I've got it all torn open and don't want to
; bump versions right now.
(ns elle.rw-register)

(defn cyclic-version-cases
  "Given a map of version graphs, returns a sequence (or nil) of cycles in that
  graph."
  [version-graphs]
  (seq
    (reduce (fn [cases [k version-graph]]
              (let [sccs (g/strongly-connected-components version-graph)]
                (->> sccs
                     (sort-by (partial reduce (fn option-compare [a b]
                                                (cond (nil? a) b
                                                      (nil? b) a
                                                      true     (min a b)))))
                     (map (fn [scc]
                            {:key k
                             :scc scc}))
                     (into cases))))
            []
            version-graphs)))
