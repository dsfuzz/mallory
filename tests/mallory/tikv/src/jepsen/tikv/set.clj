; Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

(ns jepsen.tikv.set
  (:require [jepsen
             [checker :as checker]
             [client :as client]
             [generator :as gen]]
            [jepsen.tikv
             [client :as c]]
            [jepsen.tikv.client.txn :as t]
            [clojure.tools.logging :refer :all]))

(defrecord Client [k conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node {:type "txn"})))

  (setup! [this test]
    (t/with-txn-retries
      (t/with-txn conn
        (t/put! conn k "#{}"))))

  (invoke! [_ test op]
    (t/with-txn-aborts op
      (t/with-txn conn
        (case (:f op)
          :read (assoc op
                       :type :ok,
                       :value (read-string
                               (t/get conn k)))

          :add (do (t/put! conn k (-> (t/get conn k)
                                      read-string
                                      (conj (:value op))
                                      pr-str))
                   (assoc op :type :ok))))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  {:client    (Client. "a-set" nil)
   :checker   (checker/set)
   :generator (->> (range)
                   (map (fn [x] {:type :invoke, :f :add, :value x})))
   :final-generator (gen/once {:type :invoke, :f :read, :value nil})})
