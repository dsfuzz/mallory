(ns jepsen.dqlite.control
  "Implements the Remote protocol to run commands locally."
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.shell :refer [sh]]
            [slingshot.slingshot :refer [throw+]]
            [jepsen.os.container :refer [containers]]
            [jepsen.control :as c]
            [clojure.string :refer [split-lines, trim]]
            [clojure.tools.logging :refer [info]])
  (:import (jepsen.control.core Remote)))

(defn exec
  "Execute a shell command."
  [conn-spec action]
  (let [user (System/getProperty "user.name")]
    (->> (apply sh
                "sudo" "nsenter" "-p" "-n" "-m" "-t" (get @containers (conn-spec :host))
                "su" user "-c"
                (action :cmd)
                (if-let [in (:in action)]
                  [:in in]
                  []))
         (c/throw-on-nonzero-exit))))

(defn cp
  "Copy files."
  [conn-spec src-paths dst-path]
  (doseq [src-path (flatten [src-paths])]
    (let [user (System/getProperty "user.name")
          cmd (str/join " " ["cp" (c/escape src-path) (c/escape dst-path)])]
      (->> (sh
            "sudo" "nsenter" "-p" "-n" "-m" "-t" (get @containers (conn-spec :host))
            "su" user "-c"
            cmd)
           (c/throw-on-nonzero-exit)))))


(defrecord NsenterRemote [_]
  Remote
  (connect [this conn-spec] (assoc this
                                   :conn-spec conn-spec))
  (disconnect! [this] this)
  (execute! [this _context action]
    (exec (:conn-spec this) action))
  (upload! [this _context local-paths remote-path _opts] (cp (:conn-spec this) local-paths remote-path))
  (download! [this _context remote-paths local-path _opts] (cp (:conn-spec this) remote-paths local-path)))

(def nsenter "A remote that does things via nsenter."  (NsenterRemote. nil))

(def ssh c/ssh)
