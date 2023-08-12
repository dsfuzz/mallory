; Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

(ns jepsen.tikv.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [popen]
            [clj-http.client :as http]
            [clojure.core.async :as async]
            [jepsen.tikv.util :as tu]
            [jepsen
             [core :as jepsen]
             [control :as c]
             [db :as db]
             [util :as util]]
            [jepsen.control.util :as cu]
            [slingshot.slingshot :refer [try+ throw+]]))

(def replica-count
  "This should probably be used to generate max-replicas in pd.conf as well,
              but for now we'll just write it in both places."
  3)

(def tidb-dir       "/opt/fs/tidb")
(def tidb-bin-dir   "/opt/fs/tidb/bin")
(def pd-bin         "pd-server")
(def pdctl-bin      "pd-ctl")
(def kv-bin         "tikv-server")
(def pd-config-file (str tidb-dir "/pd.conf"))
(def pd-log-file    (str tidb-dir "/pd.log"))
(def pd-stdout      (str tidb-dir "/pd.stdout"))
(def pd-pid-file    (str tidb-dir "/pd.pid"))
(def pd-data-dir    (str tidb-dir "/data/pd"))
(def kv-config-file (str tidb-dir "/kv.conf"))
(def kv-log-file    (str tidb-dir "/kv.log"))
(def kv-stdout      (str tidb-dir "/kv.stdout"))
(def kv-pid-file    (str tidb-dir "/kv.pid"))
(def kv-data-dir    (str tidb-dir "/data/kv"))
(def cov-server     "/opt/cov-server/target/release/cov-server")
(def cov-server-log (str tidb-dir "/cov-server.log"))
(def cov-server-pidfile (str tidb-dir "/cov-server.pid"))

(def client-port 2379)
(def peer-port   2380)

(defn tidb-map
  "Computes node IDs for a test."
  [test]
  (->> (:nodes test)
       (map-indexed (fn [i node]
                      [node {:pd (str "pd" (inc i))
                             :kv (str "kv" (inc i))}]))
       (into {})))

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" (name node) ":" port))

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (node-url node client-port))

(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (node-url node peer-port))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
              \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node] (str (get-in (tidb-map test) [node :pd])
                            "=" (peer-url node))))
       (str/join ",")))

(defn start-cov-server [test node]
  (c/su
   (cu/start-daemon!
    {:logfile cov-server-log
     :pidfile cov-server-pidfile
     :chdir tidb-dir}
    cov-server
    kv-stdout)))

(defn configure-pd!
  "Writes configuration file for placement driver"
  []
  (c/su (c/exec :echo (slurp (io/resource "pd.conf")) :> pd-config-file)))

(defn configure-kv!
  "Writes configuration file for tikv"
  []
  (c/su (c/exec :echo (slurp (io/resource "tikv.conf")) :> kv-config-file)))

(defn configure!
  "Write all config files."
  []
  (configure-pd!)
  (configure-kv!))

(defn pd-api-path
  "Constructs an API path for the PD located on the given node."
  [node & path-components]
  (str "http://" node ":2379/pd/api/v1/" (str/join "/" path-components)))

(defn pd-members
  "All members of the cluster"
  [node]
  (-> (pd-api-path node "members")
      (http/get {:as :json})
      :body))

(defn pd-leader
  "Gets the current PD leader."
  [node]
  (-> (pd-api-path node "leader")
      (http/get {:as :json})
      :body))

(defn pd-leader-node
  "Returns the name of the node which is the current PD leader."
  [test node]
  (let [leader-name (:name (pd-leader node))]
    (->> (tidb-map test)
         (keep (fn [[node m]]
                 (when (= leader-name (:pd m))
                   node)))
         first)))

(defn pd-endpoints
  "Constructs an initial pd cluster string for a test, like
    \"foo:2379,bar:2379,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node] (str (name node) ":" client-port)))
       (str/join ",")))

(defmacro await-http
  "Loops body until HTTP call returns, retrying 500 and 503 errors."
  [& body]
  `(loop []
     (let [res# (try+ ~@body
                      (catch [:status 500] _# ::retry)
                      (catch [:status 503] _# ::retry))]
       (if (= ::retry res#)
         (recur)
         res#))))

(defn start-pd!
  "Starts the placement driver daemon"
  [test node]
  (c/su
   (cu/start-daemon!
    {:logfile pd-stdout
     :pidfile pd-pid-file
     :chdir   tidb-dir}
    (str "./bin/" pd-bin)
    :--name                  (get-in (tidb-map test) [node :pd])
    :--data-dir              pd-data-dir
    :--client-urls           (str "http://0.0.0.0:" client-port)
    :--peer-urls             (str "http://0.0.0.0:" peer-port)
    :--advertise-client-urls (client-url node)
    :--advertise-peer-urls   (peer-url node)
    :--initial-cluster       (initial-cluster test)
    :--log-file              pd-log-file
    :--config                pd-config-file)))

(defn start-kv!
  "Starts the TiKV daemon"
  [test node]
  (c/su
   (cu/start-daemon!
    {:logfile kv-stdout
     :pidfile kv-pid-file
     :chdir   tidb-dir}
    (str "./bin/" kv-bin)
    :--pd             (pd-endpoints test)
    :--addr           (str "0.0.0.0:20160")
    :--advertise-addr (str (name node) ":" "20160")
    :--data-dir       kv-data-dir
    :--log-file       kv-log-file
    :--config         kv-config-file)))

(defn page-ready?
  "Fetches a status page URL on the local node, and returns true iff the page
    was available."
  [url]
  (try+
   (c/exec :curl :--fail url)
   (catch [:type :jepsen.control/nonzero-exit] _ false)))

(defn pd-ready?
  "Is Placement Driver ready?"
  []
  (page-ready? (str "http://127.0.0.1:" client-port "/health")))

(defn kv-ready?
  "Is TiKV ready?"
  []
  (page-ready? "http://127.0.0.1:20180/status"))

(defn restart-loop*
  "TiDB is fragile on startup; processes love to crash if they can't complete
    their initial requests to network dependencies. We try to work around this by
    checking whether the daemon is running, and restarting it if necessary.
  
    Takes a name (used for error messages and logging), a function which starts
    the node, and a status function which returns one of three states:
  
    :starting - The node is still starting up
    :ready    - The node is (hopefully) ready to serve requests
    :crashed  - The node crashed
  
    We call start, then poll the status function until we see :ready or :crashed.
    If ready, returns. If :crashed, restarts the node and tries again."
  [name start! get-status]
  (let [deadline (+ (util/linear-time-nanos) (util/secs->nanos 300))]
    ; First startup!
    (start!)

    (loop [status :init]
      (when (< deadline (util/linear-time-nanos))
        ; Out of time
        (throw+ {:type :restart-loop-timed-out, :service name}))

      (when (= status :crashed)
        ; Need to restart
        (info name "crashed during startup; restarting")
        (start!))

      ; Give it a bit
      (Thread/sleep 1000)

      ; OK, how's it doing?
      (let [status (get-status)]
        (if (= status :ready)
          ; Done
          status
          ; Still working
          (recur status))))))

(defmacro restart-loop
  "Macro form of restart-loop*: takes two forms instead of two functions."
  [name start! get-status]
  `(restart-loop* ~name
                  (fn ~'start!     [] ~start!)
                  (fn ~'get-status [] ~get-status)))

(defn start-wait-pd!
  "Starts PD, waiting for the health page to come online."
  [test node]
  (restart-loop :pd (start-pd! test node)
                (cond (pd-ready?)                       :ready
                      (cu/daemon-running? pd-pid-file)  :starting
                      true                              :crashed)))

(defn start-wait-kv!
  "Starts TiKV, waiting for the health page to come online."
  [test node]
  (restart-loop :kv (start-kv! test node)
                (cond (kv-ready?)                       :ready
                      (cu/daemon-running? kv-pid-file)  :starting
                      true                              :crashed)))

(defn stop-pd! [test node] (c/su
                            ; Faketime wrapper means we only kill the wrapper
                            ; script, not the underlying binary
                            (cu/stop-daemon! pd-bin pd-pid-file)
                            (cu/grepkill! pd-bin)))

(defn stop-kv! [test node] (c/su (cu/stop-daemon! kv-bin kv-pid-file)
                                 (cu/grepkill! kv-bin)))

(defn stop!
  "Stops all daemons"
  [test node]
  (stop-kv! test node)
  (stop-pd! test node))

(def client-types
  {:register    "raw"
   :set         "txn"
   :list-append "txn"})

(defn tikv
  "A TiKV store."
  []
  (reify db/DB
    (setup! [_ test node]
      (c/su
       (info node "installing TiKV")
       ;; Start coverage server
       (start-cov-server test node)
       ;; wait to make sure cov-server is ready
       (Thread/sleep 6000)
       (configure!)

       (try+ (start-wait-pd! test node)
             ; If we don't synchronize, KV might explode because PD isn't
             ; fully available
             (jepsen/synchronize test)
             (Thread/sleep 5000)

             (start-wait-kv! test node)
             (jepsen/synchronize test)
             (Thread/sleep 5000)

             (let [process (popen/popen ["./rpc-server"
                                         "--node" node
                                         "--port" (str (+ 8000 (tu/num-suffix node)))
                                         "--type" (client-types (:workload test))]
                                        :redirect false
                                        :dir nil
                                        :env {})
                   output (line-seq (popen/stderr process))]
               (async/go (doseq [line output]
                           (info "rpc server log:" line))))

             (catch [:type :restart-loop-timed-out] e
               (throw+ {:type :jepsen.db/setup-failed})))))

    (teardown! [_ test node]
      (c/su
       (info node "tearing down TiKV")
       (stop! test node)
       (shell/sh "pkill" "rpc-server")
       (Thread/sleep 6000)
       (c/exec :killall :cov-server :|| :true)
       (cu/stop-daemon! cov-server cov-server-pidfile)
       ; Delete everything but bin/
       (try+ (->> (cu/ls tidb-dir)
                  (remove #{"bin"})
                  (map (partial str tidb-dir "/"))
                  (c/exec :rm :-rf))
             (catch [:type :jepsen.control/nonzero-exit, :exit 2] e
               ; No such dir
               nil))))

    db/LogFiles
    (log-files [_ test node]
      [kv-log-file
       kv-stdout
       pd-log-file
       pd-stdout
       cov-server-log])

    db/Process
    (start! [_ test node]
      (start-pd! test node)
      (start-kv! test node))

    (kill! [_ test node]
      (stop-pd! test node)
      (stop-kv! test node))

    db/Pause
    (pause!  [_ test node]
      (cu/signal! pd-bin :STOP)
      (cu/signal! kv-bin :STOP))

    (resume! [_ test node]
      (cu/signal! pd-bin :CONT)
      (cu/signal! kv-bin :CONT))))
