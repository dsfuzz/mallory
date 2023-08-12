(ns jepsen.braft.db
  (:require
   [clojure.tools.logging :refer :all]
   [jepsen.db :as db]
   [jepsen.control.util :as cu]
   [jepsen.control :as c]))

(def atomic-path "/opt/fs/braft/example/atomic")
;; (def atomic-path "/mnt/charybdefs/braft/example/atomic")
(def logfile (str atomic-path "/run.log"))
(def data-dir (str atomic-path "/data"))
(def cov-server "/opt/cov-server/target/release/cov-server")
(def cov-server-log (str atomic-path "/cov-server.log"))
(def cov-server-pidfile (str atomic-path "/cov-server.pid"))

(defn boot!
  "start-up and configure atomic_server."
  [test node]
  (info node "boot cov-server")
  (c/su
   (cu/start-daemon!
    {:logfile cov-server-log
     :pidfile cov-server-pidfile
     :chdir atomic-path}
    cov-server
    logfile)
   (c/exec :sleep 5))
  (info node "boot atomic_server")
  (c/su (c/cd atomic-path
              (c/exec :bash "jepsen_control.sh" "boot")
              (c/exec :sleep 5))))

(defn start!
  "start atomic_server."
  [test node]
  (info node "start atomic_server")
  (c/su (c/cd atomic-path
              (c/exec :bash "jepsen_control.sh" "start")
              (c/exec :sleep 5))))

(defn stop!
  "stop atomic_server."
  [test node]
  (info node "stop atomic_server")
  (c/su (c/cd atomic-path
              (c/exec :bash "jepsen_control.sh" "stop")
              (c/exec :sleep 5))))

(defn kill!
  "stop atomic_server."
  [test node]
  (info node "kill atomic_server")
  (c/su (c/cd atomic-path
              (c/exec :bash "jepsen_control.sh" "kill")
              (c/exec :sleep 5))))

(defn restart!
  "restart atomic_server."
  [test node]
  (info node "restart atomic_server")
  (c/su (c/cd atomic-path
              (c/exec :bash "jepsen_control.sh" "restart")
              (c/exec :sleep 5))))

(defn add!
  "add atomic_server."
  [test node]
  (info node "add atomic_server")
  (c/su (c/cd atomic-path
              (c/exec :bash "jepsen_control.sh" "join")
              (c/exec :sleep 5))))

(defn remove!
  "remove atomic_server."
  [test node]
  (info node "remove atomic_server")
  (c/su (c/cd atomic-path
              (c/exec :bash "jepsen_control.sh" "leave")
              (c/exec :sleep 5))))

(defn db
  "atomic DB"
  []
  (reify db/DB
    (setup! [_ test node]
      (boot! test node))

    (teardown! [_ test node]
      (kill! test node)
      (c/exec :sleep 5)
      (c/exec :killall :cov-server :|| :true)
      (cu/stop-daemon! cov-server cov-server-pidfile)
      (c/exec :rm :-f cov-server-log)
      (c/exec :rm :-f cov-server-pidfile)
      (c/exec :rm :-r :-f data-dir)
      (c/exec :rm :-f logfile))

    db/LogFiles
    (log-files [_ test node]
      [logfile cov-server-log])

    db/Process
    (start! [_ test node]
      (start! test node))

    (kill! [_ test node]
      (kill! test node))

    db/Pause
    (pause! [_ test node] (c/su (cu/grepkill! :stop "atomic_server")))
    (resume! [_ test node] (c/su (cu/grepkill! :cont "atomic_server")))))