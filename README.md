# Jepsen & Mallory

Breaking distributed systems so you don't have to.

**Jepsen** is a Clojure library. A test is a Clojure program which uses the Jepsen
library to set up a distributed system, run a bunch of operations against that
system, and verify that the history of those operations makes sense. Jepsen has
been used to verify everything from eventually-consistent commutative databases
to linearizable coordination systems to distributed task schedulers. It can
also generate graphs of performance and availability, helping you characterize
how a system responds to different faults. See
[jepsen.io](https://jepsen.io/analyses) for examples of the sorts of analyses
you can carry out with Jepsen.

**Mallory** is a graybox extension to Jepsen, implemented in Rust. It hooks into
an existing Jepsen test and takes the role of the nemesis, deciding in real-time
which actions to inject and when, based on the _runtime_ behaviour of the system
under test.

# Cite Mallory
Mallory has been accepted for publication at the 30th ACM Conference on Computer and Communications Security (CCS 2023).

```
@inproceedings{mallory,
author={Ruijie Meng, George Pirlea, Abhik Roychoudhury and Ilya Sergey},
title={Greybox Fuzzing of Distributed Systems},
booktitle={Proceedings of the 30th ACM Conference on Computer and Communications Security (CCS)},
year={2023},}
```

## Design Overview

### Jepsen

A Jepsen test runs as a Clojure program on a *control node*. That program uses
SSH to log into a bunch of *db nodes*, where it sets up the distributed system
you're going to test using the test's pluggable *os* and *db*.

Once the system is running, the control node spins up a set of logically
single-threaded *processes*, each with its own *client* for the distributed
system. A *generator* generates new operations for each process to perform.
Processes then apply those operations to the system using their clients. The
start and end of each operation is recorded in a *history*. While performing
operations, a special *nemesis* process introduces faults into the system--_also
scheduled by the generator._

Finally, the DB and OS are torn down. Jepsen uses a *checker* to analyze the
test's history for correctness, and to generate reports, graphs, etc. The test,
history, analysis, and any supplementary results are written to the filesystem
under `store/<test-name>/<date>/` for later review. Symlinks to the latest
results are maintained at each level for convenience.

### Mallory

Mallory hooks into your Jepsen test and takes the place of the nemesis
generator. We use a custom version of Jepsen, modified to inform Mallory when
tests start and end and when client and nemesis operations are executed. Most
importantly, Mallory uses the nemeses defined in the Jepsen test---this requires
some modification of these nemeses, as explained in the tutorial below.

As the test executes, Mallory observes the system under test and introduces
faults with the goal of inducing behaviour not seen before.

## Documentation

This [tutorial](doc/tutorial/index.md) walks you through writing a Jepsen test
from scratch. For reference, see the [API documentation](http://jepsen-io.github.io/jepsen/).

## Setting up a Jepsen + Mallory environment

We provide a ready-made environment using Vagrant:

```bash
cd docker/
vagrant plugin install vagrant-reload   # only needed once
vagrant up
```

### Modifying an existing Jepsen test for Mallory

If you have an existing Jepsen test harness, Mallory takes the place of your
existing nemesis package and generator.

```Clojure
(:require [jepsen.mediator.wrapper :as med])

;; this should be a list of packages, as returned by
;; jepsen/nemesis/combined.clj:nemesis-packages
;; and NOT a combined package (as returned by compose-package)
;; If you have custom nemeses, you need to write a version of this yourself
;; that includes your custom nemesis.
packages      (nemesis/nemesis-packages nemesis-opts)

;; Previously, the nemesis package was obtained as such:
;; nemesis       (nemesis/nemesis-package nemesis-opts)
nemesis      (med/adaptive-nemesis packages nemesis-opts)]

;; in your test, make the nemesis generator refer to the adaptive package:
:generator
        (->> (:generator workload)
                (gen/stagger (/ (:rate opts)))
                ;; use the adaptive nemesis generator
                (gen/nemesis (:generator nemesis))
                (gen/time-limit (:time-limit opts)))
```

IMPORTANT:
- if your nemesis package only uses nemeses in Jepsen's default
  `jepsen/nemesis/combined.clj`, our distribution rewrites those so they are
  usable by Mallory;
- if you package custom nemeses, you must modify them as follows: (1) add a
  `:ops` field that returns the set of operations (and arguments) supported by
  the nemesis, and (2) add a `:dispatch` field that takes an operation type
  returned by `op` and returns an instantiated operation that can be invoked by
  the nemesis client

Here is an example nemesis adapted for use with Mallory:

```Clojure
(defn partition-package
  "A nemesis and generator package for network partitions. Options as for
  nemesis-package."
  [opts]
  (let [needed? ((:faults opts) :partition)
        db      (:db opts)
        targets (:targets (:partition opts) (partition-specs db))
        start (fn start [_ _]
                {:type  :info
                 :f     :start-partition
                 :value (rand-nth targets)})
        stop  {:type :info, :f :stop-partition, :value nil}
        gen   (->> (gen/flip-flop start (repeat stop))
                   (gen/stagger (:interval opts default-interval)))
        ;; Needed by Mallory -- to inform at start-up which operations this nemesis can perform
        ops   (cond-> []
                needed? (concat [{:f :start-partition :values (vec targets)}, {:f :stop-partition, :values [nil]}]))]
    ;; Needed by Mallory -- to transform an operation type into a specific operation
    (defn dispatch [op test ctx]
      (case (:f op)
        :start-partition  ((fn start [_ _] {:type  :info
                                            :f     :start-partition
                                            :value (or (:value op) (rand-nth targets))}) test ctx)
        :stop-partition  stop
        nil))

    {:generator       (when needed? gen)
     :final-generator (when needed? stop)
     :nemesis         (partition-nemesis db)
     :perf            #{{:name  "partition"
                         :start #{:start-partition}
                         :stop  #{:stop-partition}
                         :color "#E9DCA0"}}
     ;; these two fields are needed by Mallory
     :ops             ops
     :dispatch        dispatch}))
```

An example `nemesis-packages` function (with many custom nemesis packages):

```Clojure
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
```

A much simpler one:

```Clojure
(defn nemesis-packages
  "Builds a combined package for the given options."
  [opts]
  (->> (nc/nemesis-packages opts)
       (concat [(member-package opts)])
       (remove nil?)))
```