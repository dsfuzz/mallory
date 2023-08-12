; Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

(ns jepsen.tikv.nemesis
  "Nemeses for TiKV"
  (:require [jepsen
             [client :as client]
             [control :as c]
             [nemesis :as nemesis]
             [net :as net]
             [generator :as gen]
             [util :as util :refer [letr]]]
            [jepsen.control.util :as cu]
            [jepsen.nemesis.time :as nt]
            [jepsen.nemesis [combined :as nc]]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [clojure.pprint :refer [pprint]]
            [jepsen.tikv
             [db :as db]
             [generator :as gen-0115]]
            [clojure.tools.logging :refer :all]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn process-nemesis
  "A nemesis that can pause, resume, start, stop, and kill tidb, tikv, and pd."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (let [nodes (:nodes test)
            nodes (case (:f op)
                    ; When resuming, resume all nodes
                    (:resume-pd :resume-kv
                                :start-pd  :start-kv) nodes

                    (util/random-nonempty-subset nodes))
            ; If the op wants to give us nodes, that's great
            nodes (or (:value op) nodes)]
        (assoc op :value
               (c/on-nodes test nodes
                           (fn [test node]
                             (case (:f op)
                               :start-pd  (db/start-pd! test node)
                               :start-kv  (db/start-kv! test node)
                               :kill-pd   (db/stop-pd!  test node)
                               :kill-kv   (db/stop-kv!  test node)
                               :pause-pd  (cu/signal! db/pd-bin :STOP)
                               :pause-kv  (cu/signal! db/kv-bin :STOP)
                               :resume-pd (cu/signal! db/pd-bin :CONT)
                               :resume-kv (cu/signal! db/kv-bin :CONT)))))))

    (teardown! [this test])))

(defn schedule-nemesis
  "A nemesis that can add stress test schedulers, shuffle-leader, shuffle-region
  and random-merge."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      ; We only need a node that has the pd-ctl utility.
      (let [nodes  (take 1 (util/random-nonempty-subset (:nodes test)))
            pd-ctl (fn [& cmds]
                      ; Execute a pd-ctl command.
                     (try (c/exec :echo cmds :| (str db/tidb-bin-dir "/" db/pdctl-bin))
                          (catch RuntimeException e
                            (info "fail to " cmds))))]
        (assoc op :value
               (c/on-nodes test nodes
                           (fn [test node]
                             (case (:f op)
                               :shuffle-leader
                               (pd-ctl :sched :add :shuffle-leader-scheduler)
                               :shuffle-region
                               (pd-ctl :sched :add :shuffle-region-scheduler)
                               :random-merge
                               (pd-ctl :sched :add :random-merge-scheduler)
                               :del-shuffle-leader
                               (pd-ctl :sched :remove :shuffle-leader-scheduler)
                               :del-shuffle-region
                               (pd-ctl :sched :remove :shuffle-region-scheduler)
                               :del-random-merge
                               (pd-ctl :sched :remove :random-merge-scheduler)))))))

    (teardown! [this test])))

(defn full-nemesis
  "Merges together all nemeses"
  []
  (nemesis/compose
   {#{:start-pd  :start-kv
      :kill-pd   :kill-kv
      :pause-pd  :pause-kv
      :resume-pd :resume-kv}               (process-nemesis)
    #{:shuffle-leader  :del-shuffle-leader
      :shuffle-region  :del-shuffle-region
      :random-merge    :del-random-merge}  (schedule-nemesis)
    {:start-partition :start
     :stop-partition  :stop}               (nemesis/partitioner nil)}))

; Generators


(defn op
  "Shorthand for constructing a nemesis op"
  ([f]
   (op f nil))
  ([f v]
   {:type :info, :f f, :value v})
  ([f v & args]
   (apply assoc (op f v) args)))

(defn partition-one-gen
  "A generator for a partition that isolates one node."
  [test process]
  (op :start-partition
      (->> test :nodes nemesis/split-one nemesis/complete-grudge)
      :partition-type :single-node))

(defn partition-pd-leader-gen
  "A generator for a partition that isolates the current PD leader in a
  minority."
  [test process]
  (let [leader (db/await-http
                (db/pd-leader-node test (rand-nth (:nodes test))))
        followers (shuffle (remove #{leader} (:nodes test)))
        nodes       (cons leader followers)
        components  (split-at 1 nodes) ; Maybe later rand(n/2+1?)
        grudge      (nemesis/complete-grudge components)]
    (op :start-partition, grudge, :partition-type :pd-leader)))

(defn partition-half-gen
  "A generator for a partition that cuts the network in half."
  [test process]
  (op :start-partition
      (->> test :nodes shuffle nemesis/bisect nemesis/complete-grudge)
      :partition-type :half))

(defn partition-ring-gen
  "A generator for a partition that creates overlapping majority rings"
  [test process]
  (op :start-partition
      (->> test :nodes nemesis/majorities-ring)
      :partition-type :ring))

(defn flip-flop
  "Switches between ops from two generators: a, b, a, b, ..."
  [a b]
  (cycle [a b]))

(defn opt-mix
  "Given a nemesis map n, and a map of options to generators to use if that
  option is present in n, constructs a mix of generators for those options. If
  no options match, returns `nil`."
  [n possible-gens]
  (let [gens (reduce (fn [gens [option gen]]
                       (if (option n)
                         (conj gens gen)
                         gens))
                     []
                     possible-gens)]
    (when (seq gens)
      (gen/mix gens))))

(defn mixed-generator
  "Takes a nemesis options map `n`, and constructs a generator for all nemesis
  operations. This generator is used during normal nemesis operations."
  [n]
  ; Shorthand: we're going to have a bunch of flip-flops with various types of
  ; failure conditions and a single recovery.
  (let [o (fn [possible-gens recovery]
            ; We return nil when mix does to avoid generating flip flops when
            ; *no* options are present in the nemesis opts.
            (when-let [mix (opt-mix n possible-gens)]
              (flip-flop mix recovery)))]

    ; Mix together our different types of process crashes, partitions, and
    ; clock skews.
    (->> [(o {:kill-pd (op :kill-pd)}
             (op :start-pd))
          (o {:kill-kv (op :kill-kv)}
             (op :start-kv))
          (o {:pause-pd (op :pause-pd)}
             (op :resume-pd))
          (o {:pause-kv (op :pause-kv)}
             (op :resume-kv))
          (o {:shuffle-leader (op :shuffle-leader)}
             (op :del-shuffle-leader))
          (o {:shuffle-region (op :shuffle-region)}
             (op :del-shuffle-region))
          (o {:random-merge (op :random-merge)}
             (op :del-random-merge))
          (o {:partition-one        partition-one-gen
              :partition-pd-leader  partition-pd-leader-gen
              :partition-half       partition-half-gen
              :partition-ring       partition-ring-gen}
             (op :stop-partition))]
         ; For all options relevant for this nemesis, mix them together
         (remove nil?)
         gen/mix
         ; Introduce either random or fixed delays between ops
         ((case (:schedule n)
            (nil :random)    gen/stagger
            :fixed           gen-0115/delay-til)
          (:interval n)))))

(defn final-generator
  "Takes a nemesis options map `n`, and constructs a generator to stop all
  problems. This generator is called at the end of a test, before final client
  operations."
  [n]
  (->> (cond-> []
         (:pause-pd n)        (conj :resume-pd)
         (:pause-kv n)        (conj :resume-kv)
         (:kill-pd n)         (conj :start-pd)
         (:kill-kv n)         (conj :start-kv)
         (:shuffle-leader n)  (conj :del-shuffle-leader)
         (:shuffle-region n)  (conj :del-shuffle-region)
         (:random-merge n)    (conj :del-random-merge)

         (some n [:partition-one :partition-half :partition-ring])
         (conj :stop-partition))
       (map op)))

(defn restart-kv-without-pd-generator
  "A special generator which pauses all PD nodes, restarts all KV nodes, waits
  a bit, and unpauses PD; the cluster should recover, but a finite retry loop
  causes it to fail."
  []
  [(gen/sleep 10)
   (fn [test _] {:type :info, :f :kill-kv,  :value (:nodes test)})
   (fn [test _] {:type :info, :f :pause-pd, :value (:nodes test)})
   (op :start-kv)
   (gen/sleep 70)
   (op :resume-pd)])

(defn full-generator
  "Takes a nemesis options map `n`. If `n` has a :long-recovery option, builds
  a generator which alternates between faults (mixed-generator) and long
  recovery windows (final-generator). Otherwise, just emits faults from
  mixed-generator, or whatever special-case generator we choose."
  [n]
  (cond (:restart-kv-without-pd n)
        (restart-kv-without-pd-generator)

        (:long-recovery n)
        (let [mix     #(gen/time-limit 120 (mixed-generator n))
              recover #(gen/phases (final-generator n)
                                   (gen/sleep 60))]
          (interleave (repeatedly mix)
                      (repeatedly recover)))

        true
        (mixed-generator n)))

(defn expand-options
  "We support shorthand options in nemesis maps, like :kill, which expands to
  :kill-pd and :kill-kv. This function expands those."
  [n]
  (cond-> n
    (:kill n) (assoc :kill-pd true
                     :kill-kv true)
    (:stop n) (assoc :stop-pd true
                     :kill-kv true)
    (:pause n) (assoc :pause-pd true
                      :pause-kv true)
    (:schedules n) (assoc :shuffle-leader true
                          :shuffle-region true
                          :random-merge true)
    (:partition n) (assoc :partition-one        true
                          :partition-pd-leader  true
                          :partition-half      true
                          :partition-ring      true)))

(defn nemesis
  "Composite nemesis and generator, given test options."
  [opts]
  (let [n (expand-options (:nemesis opts))]
    {:nemesis         (full-nemesis)
     :generator       (full-generator n)
     :final-generator (final-generator n)}))


(defn nemesis-package
  "Constructs a nemesis and generators for dqlite."
  [opts]
  (let [opts (update opts :faults set)]
    (->> (concat [(nc/partition-package opts)
                  (nc/db-package opts)]
                 (:extra-packages opts))
         (remove nil?)
         nc/compose-packages)))