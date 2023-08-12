(ns jepsen.braft.client
  (:require [jepsen.client :as client]
            [jepsen.control.util :as cu]
            [jepsen.control :as c]
            [clojure.string :as str]
            [jepsen.braft.db :as db]))

(def op-timeout-ms 3000)
(def op-timeout-s (/ op-timeout-ms 1000))
(def peers "10.1.1.3:8700,10.1.2.3:8700,10.1.3.3:8700,10.1.4.3:8700,10.1.5.3:8700")

(defn atomic-get!
  "get a value for id"
  [node id]
  (c/on node
        (c/su
         (c/cd db/atomic-path
               (c/exec :timeout :-v op-timeout-s
                       "./atomic_test"
                       :-conf peers
                       :-atomic_op "get"
                       :-atomic_id id)))))

(defn atomic-set!
  "set a value for id"
  [node id value]
  (c/on node
        (c/su
         (c/cd db/atomic-path
               (c/exec :timeout :-v op-timeout-s
                       "./atomic_test"
                       :-conf peers
                       :-atomic_op "set"
                       :-atomic_val value
                       :-atomic_id id)))))

(defn atomic-cas!
  "cas set a value for id"
  [node id value1 value2]
  (try
    (c/on node
          (c/su
           (c/cd db/atomic-path
                 (c/exec :timeout :-v op-timeout-s
                         "./atomic_test"
                         :-conf peers
                         :-atomic_op "cas"
                         :-atomic_val value1
                         :-atomic_new_val value2
                         :-atomic_id id))))
    (catch Exception e
      ; For CAS failed, we return false, otherwise, re-raise the error. 
      (if (re-matches #".*atomic_cas failed.*" (str/trim (.getMessage e)))
        false
        (throw e)))))

(defn mop!
  [client [f k v :as mop]]
  (case f
    :r
    (let [resp (atomic-get! client k)]
      [f k (Long/valueOf resp)])

    :w
    (do (atomic-set! client k v)
        mop)))

(defrecord CASClient [client]
  client/Client
  (open! [this test node]
    (let [n node]
            ;(atomic-set! node k (json/generate-string 0))
      (assoc this :client node)))

  (invoke! [this test op]
    (try
      (let [txn (:value op)
            txn' (mapv (partial mop! client) txn)]
        (assoc op :type :ok :value txn'))
      (catch Exception e
        (let [msg (str/trim (.getMessage e))]
          (cond
            (re-find #".*timed out.*|.*timeout.*" msg) (assoc op :type :info, :error :timeout)
            :else (throw e))))))

  (setup! [this test])
  (teardown! [this test])
  (close! [this test]))

(defn cas-client
  "A compare and set register built around a single atomic id."
  []
  (CASClient. nil))