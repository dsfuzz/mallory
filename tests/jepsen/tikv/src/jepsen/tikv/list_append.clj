; Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

(ns jepsen.tikv.list-append
  "This test performs transactional appends and reads of various keys.
  Each key has a list of values encoded as a string."
  (:require [jepsen
             [checker :as checker]
             [client :as client]
             [generator :as gen]]
            [jepsen.tikv
             [client :as c]]
            [jepsen.tikv.client.txn :as t]
            [jepsen.tests.cycle.append :as append]
            [slingshot.slingshot :refer [try+ throw+]]
            [clojure.tools.logging :refer :all]))

(defn get
  [conn k]
  (try+ (t/get conn k)
        (catch [:type :not-found] e ; gRPC not found error
          "[]")))

(defn mop!
  "Executes a transactional micro-op on a connection. Returns the completed micro-op."
  [conn test [f k v]]
  [f k (case f
         :r (read-string (get conn k))
         :append (do (t/put! conn k (-> (get conn k)
                                        read-string
                                        (conj v)
                                        pr-str))
                     v))])

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node {:type "txn"})))

  (setup! [this test])

  (invoke! [_ test op]
    (t/with-txn-aborts op
      (let [txn       (:value op)
            txn'      (t/with-txn conn
                        (mapv (partial mop! conn test) txn))]
        (assoc op :type :ok, :value txn'))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn workload
  "See options for jepsen.tests.append/test"
  [opts]
  (assoc (append/test {:consistency-models [:read-committed :snapshot-isolation]})
                                            ; unsatisfied levels: :repeatable-read :serializable
         :client (Client. nil)))
