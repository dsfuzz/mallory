; Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

(ns jepsen.tikv.util
  "Utilities"
  (:require [slingshot.slingshot :refer [throw+]]))

(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (Long/parseLong s)))

(defn num-suffix
  [node]
  (let [len (count node)]
    (parse-long (subs node (- len 1) len))))

(defmacro handle-error!
  [value error]
  `(do
     (cond
       (nil? ~error) ~value
       ; TODO(ziyi) use iterator and map to generate these cases below.
       (not (empty? (:undetermined ~error))) (throw+ {:type :undetermined :message (:undetermined ~error)})
       (not (empty? (:not-found ~error))) (throw+ {:type :not-found :message (:not-found ~error)})
       (not (empty? (:aborted ~error))) (throw+ {:type :aborted :message (:aborted ~error)}))))
