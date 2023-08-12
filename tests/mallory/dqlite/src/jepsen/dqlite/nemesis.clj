(ns jepsen.dqlite.nemesis
  "Nemeses for Dqlite"
  (:require [jepsen [control :as c]
             [nemesis :as n]
             [util :refer [random-nonempty-subset]]
             [generator :as gen]]
            [jepsen.nemesis [combined :as nc]]
            [jepsen.dqlite.db :as db]))

(defn member-nemesis
  "A nemesis for adding and removing nodes from the cluster."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assoc op :value
             (case (:f op)
               :grow     (db/grow! test)
               :shrink   (db/shrink! test))))

    (teardown! [this test])

    n/Reflection
    (fs [_] [:grow :shrink])))

(defn member-generator
  "A generator for membership operations."
  [opts]
  (->> (gen/mix [{:type :info, :f :grow}
                 {:type :info, :f :shrink}])
       (gen/delay (:interval opts))))

(defn member-package
  "A combined nemesis package for adding and removing nodes."
  [opts]
  (when ((:faults opts) :member)
    {:nemesis   (member-nemesis opts)
     :generator (member-generator opts)
     :perf      #{{:name  "grow"
                   :fs    [:grow]
                   :color "#E9A0E6"}
                  {:name  "shrink"
                   :fs    [:shrink]
                   :color "#ACA0E9"}}
     :ops        [{:f :grow :values [nil]} {:f :shrink :values [nil]}]
     :dispatch   (fn [op test ctx] (case (:f op)
                                     :grow    {:type :info, :f :grow}
                                     :shrink  {:type :info, :f :shrink}
                                     nil))}))

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
                          :stop-node   ((fn [test _] {:type :info, :f :stop-node, :value (or (:value op) (rand-nth (:nodes test)))}) test ctx)))}))

(defn stable-nemesis
  [opts]
  (reify
    n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (do (case (:f op)
            :stable (db/stable test)
            :health (db/health test))
          (assoc op :value nil)))

    (teardown! [this test])

    n/Reflection
    (fs [_] [:stable :health])))

(defn stable-package
  [opts]
  {:nemesis (stable-nemesis opts)
   :generator nil
   :ops [{:f :stable :values [nil]} {:f :health :values [nil]}]
   :dispatch        (fn [op test ctx]
                      (case (:f op)
                        :stable {:type :info, :f :stable, :value nil}
                        :health {:type :info, :f :health, :value nil}))})

(defn nemesis-packages
  "Constructs a nemesis and generators for dqlite."
  [opts]
  (let [opts (update opts :faults set)]
    (->> (concat [(nc/partition-package opts)
                  (nc/db-package opts)
                  (member-package opts)
                  (stop-package opts)
                  (stable-package opts)]
                 (:extra-packages opts))
         (remove nil?))))

(defn nemesis-package
  "Constructs a nemesis and generators for dqlite."
  [opts]
  (nc/compose-packages (nemesis-packages opts)))