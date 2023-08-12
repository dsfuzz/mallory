; Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

(ns jepsen.tikv.client.txn
  (:require [tikv.txn.Client.client :as txnkv]
            [protojure.grpc.client.providers.http2 :as grpc.http2]
            [protojure.grpc.client.api :as grpc.api]
            [jepsen.tikv.util :as tu]
            [slingshot.slingshot :refer [try+]]))

(def ^:dynamic *txn-id* 0)

(defmacro with-txn
  [conn & body]
  `(binding [*txn-id* (begin-txn ~conn)]
     (try (let [ret# (do ~@body)]
            (do (commit! ~conn)
                ret#))
          (catch Exception e#
            (do (rollback! ~conn)
                (throw e#))))))

(defmacro capture-txn-abort
  "Converts aborted transactions to an ::abort keyword"
  [& body]
  `(try+ ~@body
         (catch [:type :not-found] e#
           ::abort)
         (catch [:type :aborted] e#
           ::abort)))

(defmacro with-txn-retries
  "Retries body on rollbacks."
  [& body]
  `(loop []
     (let [res# (capture-txn-abort ~@body)]
       (if (= ::abort res#)
         (recur)
         res#))))

(defmacro with-txn-aborts
  "Aborts body on rollbacks."
  [op & body]
  `(let [res# (capture-txn-abort ~@body)]
     (if (= ::abort res#)
       (assoc ~op :type :fail :error :conflict)
       res#)))

(defn begin-txn
  [conn]
  (let [rply @(txnkv/BeginTxn (:conn conn) {:type 0})
        error  (:error rply)
        txn-id (:txn-id rply)]
    (tu/handle-error! txn-id error))) ; TODO(ziyi) hard-coded 0, which means using begin_optimistic

(defn commit!
  [conn]
  (let [rply @(txnkv/Commit (:conn conn) {:txn-id *txn-id*})
        error (:error rply)]
    (tu/handle-error! rply error)))

(defn rollback!
  [conn]
  (let [rply @(txnkv/Rollback (:conn conn) {:txn-id *txn-id*})
        error (:error rply)]
    (tu/handle-error! rply error)))

(defn get
  [conn key]
  (let [key (str key)]
    (let [rply @(txnkv/Get (:conn conn) {:txn-id *txn-id* :key key})
          error (:error rply)
          value (:value rply)]
      (tu/handle-error! value error))))

(defn put!
  [conn key value]
  (let [key   (str key)
        value (str value)]
    (let [rply @(txnkv/Put (:conn conn) {:txn-id *txn-id* :key key :value value})
          error (:error rply)]
      (tu/handle-error! rply error))))
