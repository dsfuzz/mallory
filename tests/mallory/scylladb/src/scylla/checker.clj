(ns scylla.checker
  (:require [clojure.core.reducers :as r]
           [knossos [core :as knossos]
                    [history :as history]])
  (:import jepsen.checker.Checker))

(defn ec-history->latencies
  "TODO: what is this for? Computing latencies over some sort of eventually
  consistent... thing?"
  [threshold]
  (fn [history]
    (->> history
         (reduce (fn [[history invokes] op]
                   (cond
                                        ;New write
                     (and (= :invoke (:type op)) (= :assoc (:f op)))
                     [(conj! history op)
                      (assoc! invokes (:v (:value op))
                              [(dec (count history)) #{} (:time op)])]

                                        ; Check propagation of writes in this read
                     (and (= :ok (:type op)) (= :read (:f op)))
                     (reduce (fn [[history invokes] value]
                               (if-let [[invoke-idx nodes start-time] (get invokes value)]
                                        ; We have an invocation for this value
                                 (cond
                                   (= (count (conj nodes (:node op))) threshold)
                                   (let [invoke (get history invoke-idx)
                                        ; Compute latency
                                         l    (- (:time op) start-time)
                                         op (assoc op :latency l)]
                                     [(-> history
                                          (assoc! invoke-idx
                                                  (assoc invoke :latency l, :completion op))
                                          (conj! op))
                                      (dissoc! invokes value)])

                                   (= (count nodes) 0)
                                   [history (assoc! invokes value
                                                    [invoke-idx (conj nodes (:node op)) (:time op)])]

                                   :default
                                   [history (assoc! invokes value
                                                    [invoke-idx (conj nodes (:node op)) start-time])])
                                 [history invokes]))
                             [history invokes]
                             (vals (:value op)))

                     :default
                     [history invokes]))
                 [(transient []) (transient {})])
         first
         persistent!)))

(defn associative-map
  "Given a set of :assoc operations interspersed with :read's, verifies that
  the newest assoc'ed value for each key is present in each read, and that
  :read's contain only key-value pairs for which an assoc was attempted. The
  map should have stabilized before a :read is issued, such that all :invoke's
  have been :ok'ed, :info'ed or :fail'ed. In that way, map is more like a
  multi-phase set model than the counter model."
  []
  (reify Checker
    (check [_ test history opts]
      (loop [history (seq (history/complete history))
             reads []
             possible {}
             confirmed {}]
        (if (nil? history)
          (let [errors (remove (fn [{:keys [confirmed possible actual]}]
                                 (and (every? (fn [[k v]]
                                                (or (= v (get confirmed k))
                                                    (some #{v} (get possible k)))) actual)
                                      (every? (clojure.core/set (keys actual)) (keys confirmed))))
                               reads)]
            {:valid? (empty? errors)
             :reads reads
             :errors errors})
          (let [op (first history)
                history (next history)]
            (case [(:type op) (:f op)]
              [:ok :read]
              (recur history (conj reads {:confirmed confirmed
                                          :possible possible
                                          :actual (:value op)}) possible confirmed)

              [:invoke :assoc]
              (recur history reads (update-in possible [(:k (:value op))]
                                              conj (:v (:value op))) confirmed)

              [:fail :assoc]
              (recur history reads (update-in possible [(:k (:value op))]
                                              (partial remove (partial = (:v (:value op)))))
                     confirmed)

              [:ok :assoc]
              (recur history reads (update-in possible [(:k (:value op))]
                                              (partial remove (partial = (:v (:value op)))))
                     (assoc confirmed (:k (:value op)) (:v (:value op))))

              (recur history reads possible confirmed))))))))
