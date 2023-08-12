(ns jepsen.braft.wr
  (:require [jepsen.generator :as gen]
            [jepsen.tests.cycle.wr :as wr]
            [jepsen.braft.client :as client]))

(defn workload
  "Constructs a workload (a map with a generator, client, checker, etc) given
  an options map."
  [opts]
  (let [workload (wr/test
                  (assoc opts
                         :consistency-models [:strict-serializable]))]
    (-> workload
        (assoc :client (client/cas-client))
        (update :generator
                (fn wrap-gen [gen]
                  (gen/map (fn tag-rw [op]
                             (case (->> op :value (map first) set)
                               #{:r}      (assoc op :f :read)
                               #{:w}      (assoc op :f :write)
                               op))
                           gen))))))