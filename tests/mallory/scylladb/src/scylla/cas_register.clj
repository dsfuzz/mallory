(ns scylla.cas-register
  "Linearizable, single-register operations backed by lightweight transactions."
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [client      :as client]
             [checker     :as checker]
             [generator   :as gen]
             [independent :as independent]
             [nemesis     :as nemesis]]
            [jepsen.tests.linearizable-register :as lr]
            [knossos.model :as model]
            [qbits.alia :as alia]
            [qbits.hayt :refer :all]
            [scylla [client :as c]])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)))

(defrecord CasRegisterClient [tbl-created? conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open test node)))

  (setup! [_ test]
    (let [session (:session conn)]
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (alia/execute session (create-keyspace :jepsen_keyspace
                                                 (if-exists false)
                                                 (with {:replication {:class :SimpleStrategy
                                                                      :replication_factor 3}})))
          (alia/execute session (use-keyspace :jepsen_keyspace))
          (alia/execute session (create-table :lwt
                                              (if-exists false)
                                              (column-definitions {:id    :int
                                                                   :value :int
                                                                   :primary-key [:id]})
                                              (with {:compaction {:class (:compaction-strategy test)}})))))))

  (invoke! [_ _ op]
    (let [s (:session conn)]
      (c/with-errors op #{:read}
        (alia/execute s (use-keyspace :jepsen_keyspace))
        (case (:f op)
          :cas (let [[k [old new]] (:value op)
                     result (alia/execute s
                                          (update :lwt
                                                  (set-columns {:value new})
                                                  (where [[= :id k]])
                                                  (only-if [[:value old]]))
                                          (c/write-opts test))]
                 (c/assert-applied result)
                 (assoc op :type :ok))

          :write (let [[k v] (:value op)
                       result (alia/execute s
                                (update :lwt
                                        (set-columns {:value v})
                                        (only-if [[:in :value (range 5)]])
                                        (where [[= :id k]]))
                                (c/write-opts test))]
                   (if (c/applied? result)
                     ; Great, we're done
                     (assoc op :type :ok)

                     ; Didn't exist, back off to insert
                     (do (c/assert-applied
                           (alia/execute s (insert :lwt
                                                   (values [[:id k]
                                                            [:value v]])
                                                   (if-exists false))
                                         (c/write-opts test)))
                         (assoc op :type :ok))))

          :read (let [[k _] (:value op)
                      v     (->> (alia/execute s
                                               (select :lwt (where [[= :id k]]))
                                               (merge {:consistency :serial}
                                                      (c/read-opts test)))
                                 first
                             :value)]
                  (assoc op :type :ok :value (independent/tuple k v)))))))

  (close! [_ _]
          (c/close! conn))

  (teardown! [_ _])

  client/Reusable
  (reusable? [_ _] true))


(defn cas-register-client
  "A CAS register implemented using LWT"
  []
  (->CasRegisterClient (atom false) nil))

(defn workload
  "This workload performs read, write, and compare-and-set operations across a
  set of linearizable registers. See jepsen.tests.linearizable-register for
  more."
  [opts]
  (assoc (lr/test {:nodes (:nodes opts)
                   :model (model/cas-register)})
         :client (cas-register-client)))
