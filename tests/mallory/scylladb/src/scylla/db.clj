(ns scylla.db
  "Database setup and teardown."
  (:require [clojure [pprint :refer :all]
             [set :as set]
             [string :as str]]
            [clojure.java.io :as io]
            [clojure.java.jmx :as jmx]
            [clojure.tools.logging :refer [info]]
            [dom-top.core :as dt :refer [with-retry]]
            [jepsen
             [db        :as db]
             [util      :as util :refer [meh timeout]]
             [control   :as c :refer [| lit]]
             [client    :as client]
             [tests     :as tests]]
            [jepsen.control [net :as net]
             [util :as cu]]
            [jepsen.os.debian :as debian]
            [scylla [client :as sc]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core Session)
           (com.datastax.driver.core Cluster)
           (com.datastax.driver.core Metadata)
           (com.datastax.driver.core Host)
           (com.datastax.driver.core.policies RetryPolicy
                                              RetryPolicy$RetryDecision)
           (java.net InetAddress)))

(def cov-server-dir "/opt/fs/scylla")
(def cov-server "/opt/cov-server/target/release/cov-server")
(def cov-server-log (str cov-server-dir "/cov-server.log"))
(def cov-server-pidfile (str cov-server-dir "/cov-server.pid"))
(def log-file "/var/log/scylla/scylla.log")

(def scylla-bin
  "The full path to the scylla binary."
  "/opt/scylladb/libexec/scylla")

(def repo-file
  "Where should we put Scylla's deb repo line?"
  "/etc/apt/sources.list.d/scylla.list")

(defn dns-resolve
  "Gets the address of a hostname"
  [hostname]
  (.getHostAddress (InetAddress/getByName (name hostname))))

(defn nodetool-status-ignorable-line?
  "Can we ignore this line from nodetool status?"
  [line]
  (condp re-find line
    #"^Datacenter:"     true
    #"^=========="      true
    #"^Status=Up/Down"  true ; header
    #"^\|/ State"       true ; header
    #"--\s+Address"     true ; header
    #"^\s*$"            true
    #"^Note: "          true
    false))

(defn parse-nodetool-status-line
  "Takes a line from `nodetool status` and returns it as a map. The format of
  nodetool status is:

    Status=Up/Down
    |/ State=Normal/Leaving/Joining/Moving
    --  Address         Load       Tokens       Owns    Host ID                               Rack
    UN  192.168.122.11  598.11 KB  256          ?       06e6a8ef-41e6-4c7e-8fc0-e6418cf18654  rack1

  which we parse to something like {:status :up, :state :normal, :address ...}"
  [line]
  (when-let [[match status state address load tokens owns id rack]
             (re-find #"^([UD\?])([NLJM\?])\s+(.+?)\s+(\?|.+? bytes|.+? \wB)\s+(\d+)\s+(.+?)\s+(.+?)\s+(.+?)$" line)]
    (let [status (case status
                   "?" :unknown
                   "U" :up
                   "D" :down)
          state (case state
                  "?" :unknown
                  "N" :normal
                  "L" :leaving
                  "J" :joining
                  "M" :moving)]
      {:status  status
       :state   state
       :address address
       :load    load
       :owns    owns
       :id      id
       :rack    rack})))

(defn nodetool-status*
  "Returns the current nodetool status, as visible to the currently bound node,
  as a sequence of parsed maps."
  []
  (let [raw    (c/su (c/exec :nodetool :status))
        salient (->> raw
                     str/split-lines
                     (remove nodetool-status-ignorable-line?))
        parsed (->> salient
                    (map parse-nodetool-status-line))]
    (when (some nil? parsed)
      (throw+ {:type     :nodetool-status-parse-error
               :unparsed (remove parse-nodetool-status-line salient)
               :raw      raw}))
    parsed))

(defn nodetool-status
  "Like nodetool-status*, but enriches node maps with a :node field
  corresponding to their Jepsen node."
  [test]
  (let [addresses->nodes (->> (:nodes test)
                              (map (juxt dns-resolve identity))
                              (into {}))]
    (->> (nodetool-status*)
         (map (fn [m]
                (let [node (addresses->nodes (:address m))]
                  (assert node)
                  (assoc m :node node)))))))

(defn log-nodetool-status
  "Logs our merged view of nodetool status, for debugging purposes. Returns
  status."
  [status]
  (->> status
       (sort-by :node)
       (map (fn [{:keys [node id state status]}]
              (str node \t state \t status \t id)))
       (str/join "\n")
       (info "merged nodetool status:\n"))
  status)

(defn decommission-node!
  "Decommissions a single node."
  [test node]
  (c/on-nodes test [node]
              (fn [_ _]
                (c/su
                 (info "Decommissioning" node)
                 (info "Nodetool decommission returned"
                       (c/exec :nodetool :decommission))))))

(def remove-timeout
  "Seconds to wait for `nodetool remove` to return"
  5)

(defn remove-node!
  "Removes a single node, using `via` to execute the remove command. Node is
  represented as a map with :node and :id fields--`node` for Jepsen to identify
  the node, `id` for Scylla."
  [test via {:keys [node id]}]
  [:removed node
   (get (c/on-nodes test [via]
                    (fn [_ _]
                      (c/su
                       (info "Asking" via "to remove" node (str "(" id ")"))
                       (try+ (c/exec :timeout remove-timeout
                                     :nodetool :removenode id)
                             (catch [:exit 124] e
                               :timeout)))))
        via)])

(defn decommission-node!
  "Decommissions (politely removes) a single node."
  [test node]
  [:decommissioned node
   (get (c/on-nodes test [node]
                    (fn [_ _]
                      (c/su
                       (info "Decommissioning" node)
                       (try+ (c/exec :timeout remove-timeout
                                     :nodetool :decommission)
                             (catch [:exit 124] e
                               :timeout)))))
        node)])

(defn repair-node!
  "Issues a nodetool repair on a single node."
  [test node]
  [:repaired node
   (get (c/on-nodes test [node]
                    (fn [_ _]
                      (c/su
                       (info "Repairing" node)
                       (try+ (c/exec :timeout remove-timeout
                                     :nodetool :repair)
                             (catch [:exit 124] e
                               :timeout)))))
        node)])

(defn wait-for-recovery
  "Waits for the driver to report all nodes are up"
  [timeout-secs conn]
  (timeout (* 1000 timeout-secs)
           (throw (RuntimeException.
                   (str "Driver didn't report all nodes were up in "
                        timeout-secs "s - failing")))
           (while (->> conn
                       .getCluster
                       .getMetadata
                       .getAllHosts
                       (map #(.isUp %))
                       and
                       not)
             (Thread/sleep 500))))

(defn live-nodes
  "Get the list of live nodes from a random node in the cluster"
  [test]
  (set (some (fn [node]
               (try (jmx/with-connection {:host (name node) :port 7199}
                      (jmx/read "org.apache.cassandra.db:type=StorageService"
                                :LiveNodes))
                    (catch Exception _
                      (info "Couldn't get status from node" node))))
             (-> test :nodes set (set/difference @(:bootstrap test))
                 (#(map (comp dns-resolve name) %)) set (set/difference @(:decommission test))
                 shuffle))))

(defn joining-nodes
  "Get the list of joining nodes from a random node in the cluster"
  [test]
  (set (mapcat (fn [node]
                 (try (jmx/with-connection {:host (name node) :port 7199}
                        (jmx/read "org.apache.cassandra.db:type=StorageService"
                                  :JoiningNodes))
                      (catch Exception _
                        (info "Couldn't get status from node" node))))
               (-> test :nodes set (set/difference @(:bootstrap test))
                   (#(map (comp dns-resolve name) %)) set (set/difference @(:decommission test))
                   shuffle))))

(defn install-jdk8!
  "Scylla has a mandatory dep on jdk8, which isn't normally available in Debian
  Buster."
  []
  (info "installing JDK8")
  (c/su
    ; LIVE DANGEROUSLY
   (c/exec :wget :-qO :- "https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public" | :apt-key :add :-)
   (debian/add-repo! "adoptopenjdk" "deb  [arch=amd64] https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ buster main")
   (debian/install [:adoptopenjdk-8-hotspot])))

(defn uninstall-scylla!
  "Removes Scylla packages--e.g. in preparation to install a different version.
  Leaves the repo file in place."
  []
  (c/su
    ; NOTE: Scylla might change their packaging later; you might need to expand
    ; this list to avoid getting a mixed system. I feel like apt *should*
    ; prevent mixed versions between, say, scylla and scylla-server, but it
    ; apparently doesn't. :(
    ;
    ; TODO: maybe figure out how to find transitive scylla-only deps and remove
    ; them automatically? Autoremove might work here.
   (c/exec :apt-get :remove :-y :--purge
           :scylla
           :scylla-conf
           :scylla-kernel-conf
           :scylla-python3
           :scylla-server
           :scylla-jmx
           :scylla-tools
           :scylla-tools-core)))

(defn prep-for-version-change!
  "If the version is changing, we wipe out the apt repo file and uninstall
  existing packages."
  [test]
  (c/su
   (info "installing ScyllaDB")
    ; If the version has changed, we wipe out the apt repo file and
    ; uninstall the existing packages.
   (when (cu/exists? repo-file)
     (let [[_ prev-version] (re-find #"scylladb-([\d\.]+)"
                                     (c/exec :cat repo-file))]
       (when (not= prev-version (:version test))
         (info "Version changed from" prev-version "to" (:version test)
               "- uninstalling packages and replacing apt repo")
         (uninstall-scylla!)
         (c/exec :rm :-rf repo-file))))))

(defn install-scylla-from-apt!
  "Installs Scylla from apt, like one normally does. Creates repo file and
  calls apt-get install."
  [test]
  (c/su
   (debian/add-repo!
    "scylla"
    (str "deb  [arch=amd64] http://downloads.scylladb.com/downloads/"
         "scylla/deb/debian/scylladb-" (:version test)
         " buster non-free")
    "hkp://keyserver.ubuntu.com:80"
    "5e08fbd8b5d6ec9c")
    ; Scylla wants to install SNTP/NTP, which is going to break in
    ; containers--we skip the install here.
   (debian/install [:scylla :scylla-jmx :scylla-tools :ntp-])))

(defn install-local-files!
  "Our test can take a :local-scylla-bin or :local-deb option, which we use to
  replace files from the normal apt installation. In order of priority, we
  choose the bin, the deb, or, if neither is given, forcibly reinstall the apt
  scylla-server package to replace any previous changes."
  [test]
  (c/su
    ; Potentially install a local override
   (let [deb (:local-deb test)
         bin (:local-scylla-bin test)]
     (cond bin (do (info "Replacing" scylla-bin "with local file" bin)
                   (c/upload bin scylla-bin))

           deb (do (info "Installing local" deb "on top of existing Scylla")
                   (let [tmp  (cu/tmp-dir!)
                         file (str tmp "/scylla.deb")]
                     (try
                       (c/exec :chmod "a+rwx" tmp)
                       (c/upload deb file)
                       (c/exec :dpkg :-i file)
                       (finally
                         (c/exec :rm :-rf tmp)))))

           :else (do ; If we're NOT replacing, we need to reinstall to
                      ; override any *previously* installed bin
                   (c/exec :apt-get :install :--reinstall :scylla-server))))))

(defn install!
  "Installs ScyllaDB on the given node."
  [node test]
  (install-jdk8!)
  (prep-for-version-change! test)
  (install-scylla-from-apt! test)
  (install-local-files! test))

(defn start-cov-server [test node]
  (c/su
   (cu/start-daemon!
    {:logfile cov-server-log
     :pidfile cov-server-pidfile
     :chdir cov-server-dir}
    cov-server
    log-file)))

(defn bootstrap-seeds
  "Returns a comma-separated string of seed nodes for boostrap.
  Return all nodes to speed up bootstrap."
  [test]
  (->> (:nodes test)
       (map dns-resolve)
       (str/join ",")))

(defn join-seeds
  "Returns a comma-separated string of seed nodes to join to.
   Is used when adding a node to a non-empty cluster. We must
   exclude the node being added from the list of seeds to ensure
   the new node is streamed to before it begins serving reads and writes."
  [test node]
  (->> (disj (into #{} (:nodes test)) node)
       (map dns-resolve)
       (str/join ",")))

(defn extra-scylla-args
  "Extra scylla args which are substituted into the SCYLLA_ARGS config."
  [test]
  ; Custom logger log levels
  (->> (:logger-log-level test)
       (map (partial str "--logger-log-level "))
       (str/join " ")))

(defn configure-journalctl!
  "Sets up journalctl logging stuff"
  []
  (c/su
   (c/exec :sed :-i "s/^#RateLimitIntervalSec=.*/RateLimitInterval=1s/" "/etc/systemd/journald.conf")
   (c/exec :sed :-i "s/^#RateLimitBurst=.*/RateLimitBurst=0/" "/etc/systemd/journald.conf")
   (c/exec :systemctl :restart :systemd-journald)))

(defn configure-rsyslog!
  "Sets up rsyslog for Scylla"
  []
  (c/su
   (info "configuring scylla logging")
   (c/exec :mkdir :-p "/var/log/scylla")
   (c/exec :mkdir :-p "/opt/fs/scylla")
   (c/exec :install :-o :root :-g :adm :-m :0640 "/dev/null"
           log-file)
   (c/exec :echo (slurp (io/resource "rsyslog.d/10-scylla.conf"))
           :> "/etc/rsyslog.d/10-scylla.conf")
   (c/exec :service :rsyslog :restart)))

(defn configure-scylla-server!
  "Sets up default/scylla-server"
  [test]
  (c/su
   (c/exec :echo
           (-> (io/resource "default/scylla-server")
               slurp
               (str/replace "$EXTRA_SCYLLA_ARGS" (extra-scylla-args test)))
           :> "/etc/default/scylla-server")))

(defn configure-scylla-yaml!
  "Sets up Scylla scylla.yaml"
  [test node seeds]
  (c/su
   (c/exec :echo
           (-> (io/resource "scylla.yaml")
               slurp
               (str/replace "$SEEDS"           seeds)
               (str/replace "$LISTEN_ADDRESS"  (dns-resolve node))
               (str/replace "$RPC_ADDRESS"     (dns-resolve node))
               (str/replace "$HINTED_HANDOFF"  (str (boolean (:hinted-handoff test))))
               (str/replace "$PHI_LEVEL"       (str (:phi-level test))))
           :> "/etc/scylla/scylla.yaml")))

(defn configure-scylla!
  "Sets up Scylla config files"
  [node test]
  (info "configuring ScyllaDB")
  (configure-scylla-server! test)
  (configure-scylla-yaml! test node (bootstrap-seeds test)))

(defn configure!
  "Uploads configuration files to the current node."
  [node test]
  (configure-journalctl!)
  (configure-rsyslog!)
  (configure-scylla! node test))

(defn wipe!
  "Kills Scylla and deletes local data files."
  [db test node]
  (db/kill! db test node)
  (c/su
   (info "deleting data files")
   (meh (c/exec :rm :-rf
                 ; We leave directories in place; Scylla gets confused
                 ; without them.
                (lit "/var/lib/scylla/data/*")
                (lit "/var/lib/scylla/commitlog/*")
                (lit "/var/lib/scylla/hints/*")
                (lit "/var/lib/scylla/view_hints/*")))))

(defn disable!
  "Moves the scylla binary to a different location, preventing Scylla from
  starting. We use this during membership changes to keep nodes removed from
  the cluster, since they rejoin on restart."
  []
  (when (cu/exists? scylla-bin)
    (c/exec :mv scylla-bin (str scylla-bin ".disabled"))))

(defn enable!
  "Undo disable!"
  []
  (let [disabled (str scylla-bin ".disabled")]
    (when (cu/exists? disabled)
      (c/exec :mv disabled scylla-bin))))

(defn db
  "Sets up and tears down ScyllaDB"
  [version]
  (let [tcpdump (db/tcpdump {:ports         [9042]
                             :clients-only? true})]
    (reify db/DB
      (setup! [db test node]
        ; As a side-effect, this is where we start tracing. Sort of a hack, but
        ; we're never going to want to *disable* tracing, and tests run
        ; sequentially, so... it should be fine.
        (when (:trace-cql test) (sc/start-tracing! test))

        ;; (db/setup! tcpdump test node)
        ; Just in case we were disabled last time.
        (enable!)
        ; Right, install
        (doto node
          ;; (cond-> (:install-scylla test) (install! test))
          (configure! test))
        ;; start cov-server 
        (start-cov-server test node)
        ;; wait to make sure cov-server is ready
        (Thread/sleep 5000)
        ; And start
        (let [t1 (util/linear-time-nanos)]
          (db/start! db test node)
          ; Once bootstrapped, update scylla.yaml with correct
          ; configuration for a non-empty cluster.
          (configure-scylla-yaml! test node (join-seeds test node))
          (sc/close! (sc/await-open test node))
          (info "Scylla startup complete in"
                (float (util/nanos->secs (- (util/linear-time-nanos) t1)))
                "seconds")))

      (teardown! [db test node]
        (wipe! db test node)
        (c/su
         (Thread/sleep 6000)
         (info "deleting log files")
         (meh (c/exec :rm :-rf log-file))
         (c/exec :killall :cov-server :|| :true)
         (cu/stop-daemon! cov-server cov-server-pidfile)
         (c/exec :rm :-f cov-server-log))
        ;; (db/teardown! tcpdump test node)
        (when (:trace-cql test) (sc/stop-tracing! test)))

      db/LogFiles
      (log-files [db test node]
        [log-file cov-server-log])

      db/Process
      (start! [_ test node]
        (info "starting ScyllaDB")
        (c/su
         (c/exec :service :scylla-server :start)
         (info "started ScyllaDB")))

      (kill! [_ test node]
        (info node "stopping ScyllaDB")
        (c/su
         (cu/grepkill! "scylla-jmx")
         (cu/grepkill! "scylla")
         (try+ (c/exec :service :scylla-server :stop)
                ; Not installed yet?
               (catch [:exit 1] e)
               (catch [:exit 5] e)))
        (info node "has stopped ScyllaDB"))

      db/Pause
      (pause! [_ test node]
        (c/su (cu/grepkill! :stop "/usr/bin/scylla")))

      (resume! [_ test node]
        (c/su (cu/grepkill! :cont :scylla))))))
