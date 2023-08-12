(ns jepsen.redis.nemesis
  "Nemeses for Redis"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [db :as db]
             [generator :as gen]
             [nemesis :as n]
             [net :as net]
             [util :as util]]
            [jepsen.nemesis [time :as nt]
             [combined :as nc]]
            [jepsen.redis [db :as rdb]]))

(defn member-nemesis
  "A nemesis for adding and removing nodes from the cluster. Options:

    :db     The database to grow and shrink.

  Leave operations can either be a node name, or a map of {:remove node :using
  node}, in which case we ask a specific node to remove the other."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (info "Current membership\n" (with-out-str
                                     (pprint (rdb/get-meta-members (:db opts)))
                                     (pprint (rdb/node-state test))))
      (assoc op :value
             (case (:f op)
               :hold   nil
               :join   [(:value op) (rdb/join!  (:db opts) test (:value op))]
               :leave  [(:value op) (rdb/leave! (:db opts) test (:value op))])))

    (teardown! [this test] this)

    n/Reflection
    (fs [this] [:join :leave :hold])))

(def min-cluster-size
  "How small can the cluster get?"
  1)

(defn join-leave-gen
  "Emits join and leave operations for a DB."
  [db test process]
  (let [members (set (rdb/members db test))
        addable (remove members (:nodes test))]
    (cond ; We can add someone
      (and (seq addable) (< (rand) 0.5))
      {:type :info, :f :join, :value (rand-nth (vec addable))}

          ; We can remove someone
      (< min-cluster-size (count members))
      {:type :info, :f :leave, :value (rand-nth (vec members))}

          ; Huh, no options at all.
      true
      {:type :info, :f :hold, :value {:type :can't-change
                                      :members members
                                      :nodes (:nodes test)}})))

(defn member-generator
  "Generates join and leave operations. Options:

    :db         The DB to act on.
    :interval   How long to wait between operations."
  [opts]
  ; Feels like the generator should be smarter here, and take over choosing
  ; nodes.
  (->> (partial join-leave-gen (:db opts))
       (gen/delay (:interval opts))))

(defn island-generator
  "A generator which picks a primary, isolates it from all other nodes, then
  issues leave requests to the isolated primary asking every other node to
  leave the cluster. Options:

    :db   - The database to island."
  [opts]
  (let [db    (:db opts)
        queue (atom nil)] ; A queue of ops we're going to emit.
    (->> (reify gen/Generator
           (op [this test process]
             (or (first
                  (swap! queue
                         (fn [q]
                           (if (seq q)
                              ; Advance
                             (next q)
                              ; Empty, refill. We pick a primary and generate
                              ; a queue of ops for it.
                             (when-let [primaries (seq (db/primaries db test))]
                               (let [p (rand-nth primaries)
                                     others (remove #{p} (:nodes test))]
                                 (info "New island target is" p)
                                  ; First, partition
                                 (concat
                                  [{:type  :info
                                    :f     :start-partition
                                    :value (n/complete-grudge [[p] others])}]
                                    ; Then, remove all other nodes
                                  (map (fn [n] {:type   :info
                                                :f      :leave
                                                :value  {:remove  n
                                                         :using   p}})
                                       others))))))))
                 ; Go again
                 (Thread/sleep 1000)
                 (recur test process))))
         (gen/delay (:interval opts)))))

(defn random-sublist
  "Randomly drops elements from the given collection."
  [coll]
  (filter (fn [_] (< 0.1 (rand))) coll))

(defn mystery-generator
  "A generator for reproducing a weird fault we don't understand yet, involving
  crashes and membership changes."
  [opts]
  ; We know this is sufficient, but it's really slow to repro
  (let [db (:db opts)]
    (->> [:kill   :all
          :start  :all
          :leave  "n3"
          :kill   ["n3" "n4" "n5"]
          :start  :all
          :leave  "n1"
          :kill   :primaries
          :start  :all
          :kill   ["n2" "n3" "n5"]
          :start  :all
          ; :kill   ["n1" "n2" "n5"]
          :leave  "n5"
          :start  :all
          :leave  "n2"
          :join   "n3"
          :join   "n1"
          :kill   :all
          :start  :all]
         (partition 2)
         random-sublist
         (map (fn [[f v]] {:type :info, :f f, :value v}))
         (gen/delay (:interval opts))))
  (->> [:leave "n1"
        :leave "n2"
        :leave "n3"
        :leave "n4"
        :leave "n5"
        :kill  :all
        :start :all
        :join "n1"
        :join "n2"
        :join "n3"
        :join "n4"
        :join "n5"
        :kill :all
        :start :all]
       (partition 2)
       cycle
       (map (fn [[f v]] {:type :info, :f f, :value v}))
       (gen/delay (:interval opts))))

(defn member-package
  "A membership generator and nemesis. Options:

    :interval   How long to wait between operations.
    :db         The database to add/remove nodes from.
    :faults     The set of faults. Should include :member to activate this
                package."
  [opts]
  (when ((:faults opts) :member)
    {:nemesis   (member-nemesis opts)
     :generator (member-generator opts)
     :perf      #{{:name  "join"
                   :fs    [:join]
                   :color "#E9A0E6"}
                  {:name  "leave"
                   :fs    [:leave]
                   :color "#ACA0E9"}}}))

(defn package-for
  "Builds a combined package for the given options."
  [opts]
  (->> (nc/nemesis-packages opts)
       (concat [(member-package opts)])
       (remove nil?)
       nc/compose-packages))

(defn package
  "Takes CLI opts; constructs a nemesis and generators for Redis. Options:

    :interval   How long to wait between operations.
    :db         The database to add/remove nodes from.
    :faults     The set of faults. A special fault, :island, yields islanding
                faults."
  [opts]
  (info opts)
  ; An island fault requires both membership and partition packages, and
  ; mystery faults need members and kills...
  (let [nemesis-opts (cond (some #{:island} (:faults opts))
                           (update opts :faults conj :member :partition)

                           (some #{:mystery} (:faults opts))
                           (update opts :faults conj :member :kill)

                           true opts)
        ; Build a package for the options we have
        nemesis-package   (package-for nemesis-opts)
        ; And also for the generator faults we were explicitly asked for
        gen-package       (package-for opts)
        ; If we're asked for islanding/mystery faults, we need a special
        ; generator for those
        faults (set (:faults opts))
        ; Ugh this is a HAAACK, we can't mix island/mystery faults with others
        _   (assert (or (and (some #{:island :mystery} faults)
                             (= 1 (count faults)))
                        (not-any? #{:island :mystery} faults))
                    "Can't mix island or mystery faults with other types")
        gen (case faults
              #{:island}  (island-generator opts)
              #{:mystery} (mystery-generator opts)
              (:generator gen-package))
        ; Should do a final gen here too but I'm lazy and we don't use final
        ; gens yet.
        ]
    ; Now combine em
    {:generator       gen
     :final-generator (:final-generator gen-package)
     :nemesis         (:nemesis nemesis-package)
     :perf            (:perf nemesis-package)}))