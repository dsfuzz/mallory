; Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

(ns jepsen.tikv.register
  "Linearizable, single-register operations"
  (:require [clojure.tools.logging :refer :all]
            [slingshot.slingshot :refer [try+]]
            [jepsen
             [client :as client]
             [checker :as checker]
             [independent :as independent]
             [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [jepsen.tikv
             [client :as c]
             [util :as tu]]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test])

  (invoke! [_ test op]
    (let [[k v] (:value op)]
      (try+
       (case (:f op)
         :read  (let [value (-> conn
                                (c/get k)
                                tu/parse-long)]
                  (assoc op :type :ok :value (independent/tuple k value)))
         :write (do (c/put! conn k v)
                    (assoc op :type :ok)))
       (catch [:type :not-found] e ; gRPC not found error
         (assoc op :type :fail :error :not-found))
       (catch [:type :aborted] e ; gRPC aborted error
         (assoc op :type :fail :error :aborted)))))

  (teardown! [this test])

  (close! [_ test]
    (c/close! conn)))

(defn r   [_ _] {:type :invoke, :f :read,  :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas,   :value [(rand-int 5) (rand-int 5)]})

(defn workload
  "Tests linearizable reads, writs, and compare-and-set operations
  on one key."
  [opts]
  {:client (Client. nil)
   :checker (independent/checker
             (checker/compose
              {:linear (checker/linearizable
                        {:model     (model/cas-register)
                         :algorithm :linear})
               :timeline (timeline/html)}))
   :generator (independent/concurrent-generator
               10
               (range)
               (fn [k]
                 (->> (gen/reserve 5 r (gen/mix [w])) ; change to (gen/mix [w cas]) when CaS operation is implemented
                      (gen/stagger (/ (:rate opts)))
                      (gen/limit (:ops-per-key opts)))))})
