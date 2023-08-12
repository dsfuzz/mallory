(ns jepsen.dqlite.client
  "Helper functions for interacting with the test Dqlite application."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [slingshot.slingshot :refer [try+ throw+]]
            [clj-http.client :as http]
            [jepsen.util :as util :refer [timeout]])
  (:import (java.net ConnectException SocketException SocketTimeoutException)))

(defn endpoint
  "The root HTTP URL of the test Dqlite application API endpoint on a node."
  [node]
  (str "http://" (name node) ":" 8080))

(defn open
  "Opens a connection to the given node. TODO: use persistent HTTP connections."
  [test node]
  {:endpoint        (endpoint node)
   :request-timeout (* 100 (:latency test))})

(defn request
  "Perform an API request"
  [conn method path & [opts]]
  (let [url (str (:endpoint conn) path)
        timeout (:request-timeout conn)]
    ;(util/timeout (* 2 timeout)
    ;              (throw+ {:type ::no-seriously-timeout})
       (let [response
           (str (:body (http/request
                          (merge {:method method
                                  :url url
                                  :socket-timeout timeout
                                  :connection-timeout timeout}
                                 opts))))]
        (if (str/includes? response "Error")
          (throw+ {:msg response})
          (edn/read-string response)))))

(defn leader
  "Return the node name of the current Dqlite leader."
  [test node]
  (let [conn (open test node)
        leader (request conn "GET" "/leader")]
    (if (= "" leader)
      (throw+ {:msg "no leader"})
      leader)))

(defn members
  "Return the names of the current cluster members."
  [test node]
  (let [conn (open test node)]
    (request conn "GET" "/members")))

(defn remove-member!
  "Remove a cluster member."
  [test node old-node]
  (let [conn (open test node)
        body (str old-node)]
    (request conn "DELETE" "/members" {:body body})))

(defn ready
  "Return whether the node is fully ready."
  [test node]
  (let [conn (open test node)]
    (request conn "GET" "/ready")))

(defn stable
  ([test node]
   (let [conn (open test node)]
     (request conn "GET" "/stable")))
  ([test node _]
   (let [conn (open test node)]
     (request conn "GET" "/health"))))

(defmacro with-errors
  "Takes an operation and a body; evals body, turning known errors into :fail
  or :info ops."
  [op & body]
  `(try+ ~@body
         (catch [:msg "Error: database is locked"] e#
           (assoc ~op :type :fail, :error :locked))
         (catch [:msg "Error: sql: database is closed"] e#
           (assoc ~op :type :fail, :error :database-closed))
         (catch [:msg "Error: checkpoint in progress"] e#
           (assoc ~op :type :fail, :error :checkpoint-in-progress))
         (catch [:msg "Error: no more rows available"] e#
           (assoc ~op :type :fail, :error :no-more-rows-available))
         (catch [:msg "Error: context deadline exceeded"] e#
           (assoc ~op :type :info, :error :timeout))
         (catch [:msg "Error: disk I/O error"] e#
           (assoc ~op :type :info, :error :disk-io-error))
         (catch [:msg "Error: failed to create dqlite connection: no available dqlite leader server found"] e#
           (assoc ~op :type :fail, :error :unavailable))
         (catch [:msg "Error: driver: bad connection"] e#
           (assoc ~op :type (if (= (:f ~op) :read) :fail :info), :error :bad-connection))
         (catch (and (:msg ~'%)
                     (re-find #"receive: header: EOF" (:msg ~'%)))
           e#
           (assoc ~op :type :info, :error :receive-header-eof))
         (catch [:type ::no-seriously-timeout] e#
           (assoc ~op :type :info, :error :no-seriously-timeout))
         (catch SocketTimeoutException e#
           (assoc ~op :type (if (= (:f ~op) :read) :fail :info), :error :connection-timeout))
         (catch SocketException e#
           (assoc ~op :type (if (= (:f ~op) :read) :fail :info), :error :connection-error))
         (catch ConnectException e#
           (assoc ~op :type :fail, :error :connection-refused))
         (catch org.apache.http.NoHttpResponseException e#
           (assoc ~op :type :info, :error :no-http-response))))
