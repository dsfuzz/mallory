(ns scylla.write-isolation
  "This is a pair of workloads designed to verify a pair of Scylla claims from
  https://docs.scylladb.com/getting-started/dml

  > All updates for an INSERT are applied atomically and in isolation.

  > In an UPDATE statement, all updates within the same partition key are
  > applied atomically and in isolation.

  > All updates in a BATCH belonging to a given partition key are performed in isolation.

  To check this, we constrain our generator such that every update is an
  update to the same set of keys; each update picks exactly one value. Since
  updates are applied in isolation, we should never observe a mixed state.

  We have two variants of this workload: one where keys are cells in a single
  row, and one where keys are rows, updated via BATCH."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util]]
            [knossos.op :as op]
            [scylla [client :as c]
                    [wr-register :as wr-register]]
            [qbits [alia :as a]
                   [hayt :as h]]))

(def key-range-size
  "How many keys per range?"
  3)

(def num-key-ranges
  "How many key ranges total?"
  3)

(defn key-range
  "Returns a collection of keys for a read or write txn to operate on."
  []
  (let [lower (* (rand-int num-key-ranges) key-range-size)
        upper (+ lower key-range-size)]
    (shuffle (range lower upper))))

(defn same-write?
  "Are all these values part of the same write?"
  [values]
  ; Handle nils
  (or (apply = values)
      ; Or negative/positive variants
      (apply = (map #(Math/abs %) values))))

(defn writes
  "Constructs txns like [[:w 0 1] [:w 1 -1] [:w 2 -1] [:w 3 1]]"
  []
  (->> (range)
       (map (fn [v]
              {:f     :write
               :value (mapv (fn [k]
                              (let [v (* v (rand-nth [1 -1]))]
                                [:w k v]))
                            (key-range))}))))

(defn reads
  "Constructs txns like [[:r 0 nil] [:r 1 nil] ...]"
  []
  (fn [] {:f      :read
          :value  (mapv (fn [k] [:r k nil]) (key-range))}))

(defn generator
  []
  (gen/mix [(writes) (reads)]))

(defn checker
  "Looks for successful reads where not every value is from the same write."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [errs (->> history
                      (filter op/ok?)
                      (filter (comp #{:read} :f))
                      (keep (fn [op]
                              (when (not (same-write?
                                           (map #(nth % 2) (:value op))))
                                op))))]
        {:valid?      (empty? errs)
         :error-count (count errs)
         :errors      errs}))))

(defn workload
  [opts]
  {:client    (wr-register/->Client nil)
   :generator (generator)
   :checker   (checker)})

(defn single-row-workload
  "This variant of the test uses a single row to store values."
  [opts]
  (assoc (workload opts) :client (wr-register/->SingleRowClient nil)))

(defn single-write-generator
  "We generate a write for a single key range, and interleave it with several
  reads of that same key range, then move on to a new range."
  []
  (->> (iterate (partial + key-range-size) 0)
       (mapcat (fn [lower]
                 (let [ks (range lower (+ lower key-range-size))
                       w  {:f :write, :value (mapv (fn [k] [:w k  1])   ks)}
                       r  {:f :read,  :value (mapv (fn [k] [:r k nil]) ks)}]
                   [r r r r r w r r r r r r r r r r]))))) ; it's nicki minaj 🤪

(defn single-write-workload
  "This variant writes each key range only once. Scylla theoretically
  (https://github.com/scylladb/scylla/issues/2379#issuecomment-301160270) might
  allow reads to observe partly-applied batches, but some cursory testing
  hasn't revealed that behavior yet."
  [opts]
  (assoc (workload opts)
         :generator (single-write-generator)))
