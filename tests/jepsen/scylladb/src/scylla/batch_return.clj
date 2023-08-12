(ns scylla.batch-return
  "The return values of batch updates seem... weird. This test aims to figure
  out what they are."
  (:refer-clojure :exclude [read])
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util :refer [map-vals]]]
            [knossos [history :as history]
                     [op :as op]]
            [scylla [client :as c]]
            [qbits [alia :as a]
                   [hayt :as h]]))

(def table-count
  "How many tables should we spread updates across? We can only do conditional
  updates across one table, sadly; maybe we can raise this later."
  1)

(defn all-tables
  "A vector of all tables in this test."
  []
  (mapv (partial str "batch_") (range table-count)))

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
        (doseq [table (all-tables)]
          (a/execute s (h/create-table
                         table
                         (h/if-exists false)
                         (h/column-definitions {:part         :int
                                                :key          :int
                                                ; We can't do LWT without SOME
                                                ; kind of IF statement (why?),
                                                ; so we leave a trivial null
                                                ; column here.
                                                :lwt_trivial  :int
                                                :int1         :int
                                                :int2         :int
                                                :primary-key  [:part :key]})
                         (h/with {:compaction {:class (:compaction-strategy test)}})))))))

  (invoke! [this test op]
    (let [s (:session conn)]
      (c/with-errors op #{}
        (a/execute s (h/use-keyspace :jepsen_keyspace))
        (let [queries (map (fn update-query [{:keys [table key updates]}]
                             (h/update table
                                       (h/set-columns updates)
                                       (h/where [[= :part 0]
                                                 [= :key key]])
                                       (h/only-if [[= :lwt_trivial nil]])))
                           (:value op))
              query (h/batch (apply h/queries queries))
              _ (info :query (h/->raw query))
              res (a/execute s query (c/write-opts test))
              ; Rename [applied] so we don't generate illegal EDN
              res (map (fn [row]
                         (-> row
                             (assoc :applied (c/applied-kw row))
                             (dissoc c/applied-kw)))
                       res)]
          (assoc op :type :ok
                 :value {:query  (:value op)
                         :result res})))))

  (close! [this test]
    (c/close! conn))

  (teardown! [this test])

  client/Reusable
  (reusable? [_ _] true))


(defn rand-update
  "Returns a random [column-key, val] update which could be applied to a single
  row."
    []
    (rand-nth [[:int1 (rand-int 10)]
               [:int2 (rand-int 10)]]))

(defn generator
  "We generate ops like {:f :batch, :value [{:key k1, :updates k1-updates}
  ...], where k1 is a row key, and k1-updates is a map of column keys to values
  to set--as would be received by h/set-columns"
  []
  {:f     :batch
   :value (->> (repeatedly (partial rand-int 10))
               (take (rand-int 5))
               (map (fn key-updates [k]
                      {:table   (rand-nth (all-tables))
                       :key     k
                       :updates (->> (repeatedly rand-update)
                                     ; We can't generate empty SET clauses
                                     (take (inc (rand-int 5)))
                                     (into {}))})))})

(defn op-errors
  "Takes a completion operation and yields nil if it looks OK, or a collection
  of error maps if the query and result don't seem to line up."
  [op]
  (let [{:keys [query result]} (:value op)
        ; This function returns true if expected and actual are equal, OR if
        ; actual is nil."
        =-or-nil? (fn [expected actual]
                    (or (nil? actual)
                        (= actual expected)))
        different? (complement =-or-nil?)]
    (cond-> []
      (not= (count query) (count result))
      (conj {:type      :unexpected-row-count
             :expected  (count query)
             :received  (count result)
             :query     query
             :result    result})

      (some boolean (map different? (map :key query) (map :key result)))
      (conj {:type     :different-key-in-position
             :expected (map :key query)
             :received (map :part result)
             :query    query
             :result   result})

      ; This is expected behavior: we return *previous* keys, rather than
      ; resulting keys, which means newly inserted rows will have nil keys.
      ;(or (some nil? (map :part result))
      ;    (some nil? (map :key result)))
      ;(conj {:type      :nil-key-or-part
      ;       :nil-key   (remove :key result)
      ;       :nil-part  (remove :part result)
      ;       :query     query
      ;       :result    result})
      )))

(defn checker
  "I have no idea what this does yet."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [ok   (filter op/ok? history)
            errs (mapcat op-errors ok)
            err-freqs (->> errs
                           (map :type)
                           frequencies
                           (map-vals #(float (/ % (count ok)))))]
        {:valid?      (empty? errs)
         :errors      errs
         :frequencies err-freqs}))))

(defn workload
  [opts]
  {:client      (Client. nil)
   :generator   generator
   :checker     (checker)})
