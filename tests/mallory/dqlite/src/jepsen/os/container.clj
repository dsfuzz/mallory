(ns jepsen.os.container
  "Set up an isolated node container using Linux namespaces."
  (:use clojure.tools.logging)
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.java.shell :refer [sh]]
            [jepsen.util :refer [meh]]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.net :as net]))

(def prefix "jepsen-")

(def bridge (str prefix "br"))

(defn exec
  "Execute a shell command without entering the node namespace."
  [& commands]
  (->> commands
       (map c/escape)
       (apply sh)
       c/throw-on-nonzero-exit
       c/just-stdout))

(def unshare-command
  (c/lit "unshare -p -n -m -f --kill-child --mount-proc sleep inf > /dev/null 2>&1 & jobs -p"))

(def containers
  "Map node names to their container PID."
  (atom {}))

(def dir "/opt/jepsen")

(def os
  (reify os/OS
    (setup! [_ test node]
      (info "Setting up container")

      (let [nodes  (:nodes test)
            i        (+ (.indexOf nodes node) 1)
            veth1    (str prefix "veth" i)
            veth2    (str prefix "br-veth" i)
            addr     (str "10.2.1.1" i "/24")
            ppid     (exec :sudo :sh :-c unshare-command)
            ;; The call to ps is a bit racey.
            sleep    (exec :sleep :0.2)
            pid      (str/trim (exec :ps :-o :pid= :--ppid ppid))
            user     (System/getProperty "user.name")
            node-dir (str dir "/" node)]

        (swap! containers assoc node pid)

        ;; Set up networking.
        (exec :sudo :ip :link :add veth1 :type :veth :peer :name veth2)
        (exec :sudo :ip :link :set veth1 :netns pid)
        (exec :sudo :nsenter :-p :-n :-m :-t pid :ip :addr :add addr :dev veth1)
        (exec :sudo :nsenter :-p :-n :-m :-t pid :ip :link :set :dev veth1 :up)
        (exec :sudo :nsenter :-p :-n :-m :-t pid :ip :link :set :dev :lo :up)
        (exec :sudo :nsenter :-p :-n :-m :-t pid :ip :route :add :default :via "10.2.1.1")
        (exec :sudo :ip :link :set veth2 :up)
        (exec :sudo :ip :link :set veth2 :master bridge)

        ;; Set up /opt
        (exec :sudo :mkdir :-p node-dir)
        (exec :sudo :nsenter :-p :-n :-m :-t pid :mount :--bind node-dir "/opt"))

      (meh (net/heal! (:net test) test)))

    (teardown! [_ test node]
      (info "Tearing down container")
      (let [pid (get @containers node)]
        (when-not (= pid "")
          (meh (exec :sudo :kill :-9 pid))
          ;; TODO: find a better way to wait for kernel to clean up everything.
          (Thread/sleep 1000))))))
