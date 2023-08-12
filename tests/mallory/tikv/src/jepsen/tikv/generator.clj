; Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

; This code is copied from https://github.com/jepsen-io/jepsen/blob/d0f9233b1d1399641add865ba3bd44178e1cb6fa/jepsen/src/jepsen/generator.clj#L226
; for delay-til.
(ns jepsen.tikv.generator
  "Generates operations for a test. Generators are composable, stateful objects
    which emit operations for processes until they are exhausted, at which point
    they return nil. Generators may sleep when generating operations, to delay
    the rate at which the test proceeds
  
    Generators do *not* have to emit a :process for their operations; test
    workers will take care of that.
  
    Every object may act as a generator, and constantly yields itself.
  
    Big ol box of monads, really."
  (:refer-clojure :exclude [map concat delay seq filter await])
  (:require [jepsen.util :as util]
            [knossos.history :as history]
            [clojure.core :as c]
            [clojure.walk :as walk]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [warn info]]
            [slingshot.slingshot :refer [throw+]]
            ;; [tea-time.core :as tt]
            )
  (:import (java.util.concurrent.atomic AtomicBoolean)
           (java.util.concurrent.locks LockSupport)
           (java.util.concurrent BrokenBarrierException
                                 CyclicBarrier)))

(defn next-tick-nanos
  "Given a period `dt` (in nanos), beginning at some point in time `anchor`
  (also in nanos), finds the next tick after time `now`, such that the next
  tick is separate from anchor by an exact multiple of dt. If now is omitted,
  defaults to the current time."
  ([anchor dt]
   (next-tick-nanos anchor dt (util/linear-time-nanos)))
  ([anchor dt now]
   (+ now (- dt (mod (- now anchor) dt)))))

(defn sleep-til-nanos
  "High-resolution sleep; takes a time in nanos, relative to System/nanotime."
  [t]
  (while (< (+ (System/nanoTime) 10000) t)
    (LockSupport/parkNanos (- t (System/nanoTime)))))

(defprotocol Generator
  (op [gen test process] "Yields an operation to apply."))

(defn op-and-validate
  "Wraps `op` to ensure we produce a valid operation for our
  worker. Re-throws any exception we catch from the generator."
  [gen test process]
  (let [op (op gen test process)]
    (when-not (or (nil? op) (map? op))
      (throw+ {:type :invalid-op
               :gen  gen
               :op   op}))
    op))

(extend-protocol Generator
  nil
  (op [this test process] nil)

  Object
  (op [this test process] this)

  ; Fns can generate ops by being called with test and process, or with no args
  clojure.lang.AFunction
  (op [f test process]
    (try
      (f test process)
      (catch clojure.lang.ArityException e
        (f)))))

(defmacro defgenerator
  "Like deftype, but the fields spec is followed by a vector of expressions
    which are used to print the datatype, and the Generator protocol is implicit.
    For instance:
  
        (defgenerator Delay [dt]
          [(str dt \" seconds\")] ; For pretty-printing
          (op [this test process] ; Function body
            ...))
  
    Inside the print-forms vector, every occurrence of a field name is replaced
    by (.some-field a-generator), so don't get fancy with let bindings or
    anything."
  [name fields print-forms & protocols-and-functions]
  (let [field-set (set fields)
        this-sym  (gensym "this")
        w-sym     (gensym "w")]
    `(do ; Standard deftype, just insert the generator and drop the print forms
       (deftype ~name ~fields Generator ~@protocols-and-functions)

           ; For prn/str, we'll generate a defmethod
       (defmethod print-method ~name
         [~this-sym ^java.io.writer ~w-sym]
         (.write ~w-sym
                 ~(str "(gen/" (.toLowerCase (clojure.core/name name))))
         ~@(mapcat (fn [field]
                         ; Rewrite field expression, changing field names to
                         ; instance variable getters
                     (let [field (walk/prewalk
                                  (fn [form]
                                    (if (field-set form)
                                           ; This was a field
                                      (list (symbol (str "." form))
                                            this-sym)
                                           ; Something else
                                      form))
                                  field)]
                           ; Spaces between fields
                       [`(.write ~w-sym " ")
                        `(print-method ~field ~w-sym)]))
                   print-forms)
         (.write ~w-sym ")")))))

(defgenerator DelayTil
  [dt precache? anchor gen]
  [(util/nanos->secs dt) precache? gen]
  (op [_ test process]
      (if precache?
        (let [op (op gen test process)]
          (sleep-til-nanos (next-tick-nanos anchor dt))
          op)
        (do (sleep-til-nanos (next-tick-nanos anchor dt))
            (op gen test process)))))

(defn delay-til
  "Operations are emitted as close as possible to multiples of dt seconds from
    some epoch. Where `delay` introduces a fixed delay between completion and
    invocation, delay-til attempts to schedule invocations as close as possible
    to the same time. This is useful for triggering race conditions.
  
    If precache? is true (the default), will pre-emptively request the next
    operation from the underlying generator, to eliminate jitter from that
    generator."
  ([dt gen]
   (delay-til dt true gen))
  ([dt precache? gen]
   (let [anchor (System/nanoTime)
         dt     (util/secs->nanos dt)]
     (DelayTil. dt precache? anchor gen))))

