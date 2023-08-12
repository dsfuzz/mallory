(ns jepsen.braft.nemesis
  "Nemeses for Braft"
  (:require [jepsen [control :as c]
             [nemesis :as n]
             [util :refer [random-nonempty-subset]]
             [generator :as gen]]
            [jepsen.nemesis [combined :as nc]]
            [jepsen.braft.db :as db]))

(defn stop-generator
  [opts]
  (let [stop (fn [test _] {:type :info, :f :stop-node, :value (rand-nth (:nodes test))})
        start (fn [test _] {:type :info, :f :start-node, :value nil})]
    (->> (gen/mix [stop stop start])
         (gen/stagger (:interval opts)))))

(defn stop-nemesis
  "A nemesis which responds to stop-node and start-node by politely
  stopping and starting the dqlite process."
  [opts]
  (reify
    n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assoc op :value
             (case (:f op)
               :start-node (c/on-nodes test db/start!)
               :stop-node  (c/on-nodes test [(:value op)] db/stop!))))

    (teardown! [this test])

    n/Reflection
    (fs [this]
      #{:start-node :stop-node})))

(defn stop-package
  "A nemesis package for politely stopping and restarting nodes."
  [opts]
  (when ((:faults opts) :stop)
    {:nemesis         (stop-nemesis opts)
     :generator       (stop-generator opts)
     :final-generator {:type :info, :f :start-node, :value nil}
     :perf            #{{:name  "stop"
                         :start #{:stop-node}
                         :stop  #{:start-node}
                         :color "#86DC68"}}
     :ops             [{:f :start-node :values [nil]} {:f :stop-node :values [:random]}]
     :dispatch        (fn [op test ctx]
                        (case (:f op)
                          :start-node  {:type :info, :f :start-node, :value nil}
                          :stop-node   ((fn [test _] {:type :info, :f :stop-node, :value (rand-nth (:nodes test))}) test ctx)))}))

(defn nemesis-packages
  "Constructs a nemesis and generators for dqlite."
  [opts]
  (let [opts (update opts :faults set)]
    (->> (concat [(nc/partition-package opts)
                  (nc/db-package opts) 
                  (stop-package opts)]
                 (:extra-packages opts))
         (remove nil?))))

(defn nemesis-package
  "Constructs a nemesis and generators for dqlite."
  [opts]
  (nc/compose-packages (nemesis-packages opts)))