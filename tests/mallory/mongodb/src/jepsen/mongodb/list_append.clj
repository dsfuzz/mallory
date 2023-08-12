(ns jepsen.mongodb.list-append
  "Elle list append workload"
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [with-retry]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [util :as util :refer [timeout]]]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.append :as list-append]
            [jepsen.mongodb [client :as c]]
            [slingshot.slingshot :as slingshot])
  (:import (java.util.concurrent TimeUnit)
           (com.mongodb TransactionOptions
                        ReadConcern
                        ReadPreference
                        WriteConcern)
           (com.mongodb.client.model Filters
                                     UpdateOptions)))

(def db-name   "jepsendb")
(def coll-name "jepsencoll")

(defn txn-options
  "Constructs options for this transaction."
  [test txn]
  ; Transactions retry for well over 100 seconds and I cannot for the life of
  ; me find what switch makes that timeout shorter. MaxCommitTime only affects
  ; a *single* invocation of the transaction, not the retries. We work around
  ; this by timing out in Jepsen as well.
  (cond-> (TransactionOptions/builder)
    true (.maxCommitTime 5 TimeUnit/SECONDS)

    ; MongoDB *ignores* the DB and collection-level read and write concerns
    ; within a transaction, which seems... bad, because it actually
    ; *downgrades* safety if you chose high levels at the db or collection
    ; levels! We have to set them here too.
    (:txn-read-concern test)
    (.readConcern (c/read-concern (:txn-read-concern test)))

    (and (:txn-write-concern test)
         ; If the transaction is read-only, and we have
         ; no-read-only-txn-write-concern set, we don't bother setting the write
         ; concern.
         (not (and (every? (comp #{:r} first) txn)
                   (:no-read-only-txn-write-concern test))))
    (.writeConcern (c/write-concern (:txn-write-concern test)))

    true .build))

(defn apply-mop!
  "Applies a transactional micro-operation to a connection."
  [test db session [f k v :as mop]]
  (let [coll (c/collection db coll-name)]
    ;(info (with-out-str
    ;        (println "db levels")
    ;        (prn :sn-rc ReadConcern/SNAPSHOT)
    ;        (prn :ma-rc ReadConcern/MAJORITY)
    ;        (prn :db-rc (.getReadConcern db))
    ;        (prn :ma-wc WriteConcern/MAJORITY)
    ;        (prn :db-wc (.getWriteConcern db))))
    (case f
      :r      [f k (vec (:value (c/find-one coll session k)))]
      :append (let [filt (Filters/eq "_id" k)
                    doc  (c/->doc {:$push {:value v}})
                    opts (.. (UpdateOptions.) (upsert true))
                    res  (if session
                           (.updateOne coll session filt doc opts)
                           (.updateOne coll filt doc opts))]
                ;(info :res res)
                mop))))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node (if (:sharded test)
                                     c/mongos-port
                                     c/shard-port))))

  (setup! [this test]
    ; Collections have to be predeclared; transactions can't create them.
    (with-retry [tries 5]
      (let [db (c/db conn db-name test)]
        (when (:sharded test)
          (c/admin-command! conn {:enableSharding db-name}))

        (let [coll (c/create-collection! db coll-name)]
          (info "Collection created")
          (when (:sharded test)
            ; Shard it!
            (c/admin-command! conn
                              {:shardCollection  (str db-name "." coll-name)
                               :key              {:_id :hashed}
                               ; WIP; gotta figure out how we're going to
                               ; generate queries with the shard key in them.
                               ;:key              {(case (:shard-key test)
                               ;                     :id :_id
                               ;                     :value :value)
                               ;                   :hashed}
                               :numInitialChunks 7})
            (info "Collection sharded"))))
      (catch com.mongodb.MongoNotPrimaryException e
        ; sigh, why is this a thing
        nil)
      (catch com.mongodb.MongoSocketReadTimeoutException e
        (if (pos? tries)
          (do (info "Timed out sharding DB and creating collection; waiting to retry")
              (Thread/sleep 5000)
              (retry (dec tries)))
          (throw e)))))

  (invoke! [this test op]
    (let [txn (:value op)]
      (c/with-errors op
        (timeout 5000 (assoc op :type :info, :error :timeout)
          (let [txn' (if (and (<= (count txn) 1)
                              (not (:singleton-txns test)))
                       ; We can run without a transaction
                       (let [db (c/db conn db-name
                                      {:read-concern  (:read-concern test)
                                       :write-concern (:write-concern test)})]
                         [(apply-mop! test db nil (first txn))])

                       ; We need a transaction
                       (let [db (c/db conn db-name test)]
                         (with-open [session (c/start-session conn)]
                           (let [opts (txn-options test (:value op))
                                 body (c/txn
                                        ;(info :txn-begins)
                                        (mapv (partial apply-mop!
                                                       test db session)
                                              (:value op)))]
                             (.withTransaction session body opts)))))]
            (assoc op :type :ok, :value txn'))))))

  (teardown! [this test])

  (close! [this test]
    (.close conn)))

(defn workload
  "A generator, client, and checker for a list-append test."
  [opts]
  (assoc (list-append/test {:key-count          10
                            :key-dist           :exponential
                            ;:key-dist           :uniform
                            :max-txn-length     (:max-txn-length opts 4)
                            :max-writes-per-key (:max-writes-per-key opts)
                            :consistency-models [:strong-snapshot-isolation]
                            :cycle-search-timeout 30000})
         :client (Client. nil)))
