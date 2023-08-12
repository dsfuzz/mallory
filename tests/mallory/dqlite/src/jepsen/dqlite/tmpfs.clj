(ns jepsen.dqlite.tmpfs
  "Provides a database and nemesis package which can work together to fill up
  disk space."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as nem]
                    [util :as util :refer [meh random-nonempty-subset]]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defrecord DB [dir size-mb]
  db/DB
  (setup! [this test node]
    (info "Setting up tmpfs at" dir)
    (c/su (c/exec :mkdir :-p dir)
          (c/exec :chmod 777 dir)
          (c/exec :mount :-t :tmpfs :tmpfs dir
                  :-o (str "size=" size-mb "M,mode=0755"))))

  (teardown! [this test node]
    (info "Unmounting tmpfs at" dir)
    (c/su (meh (c/exec :umount :-l dir)))))

(def balloon-file
  "The name of the file which we use to eat up all available disk space."
  "jepsen-balloon")

(defn fill!
  "Inflates the balloon file, causing the given DB to run out of disk space."
  [db]
  (c/su (try+ (c/exec :dd "if=/dev/zero" (str "of=" (:dir db) "/" balloon-file))
              (catch [:type :jepsen.control/nonzero-exit] e
                ; Normal, disk is full!
                )))
  :filled)

(defn free!
  "Releases the balloon file's data for the given DB."
  [db]
  (c/su (c/exec :rm :-f (str (:dir db) "/" balloon-file)))
  :freed)

(defrecord Nemesis [db]
  nem/Nemesis
  (setup! [this test] this)

  (invoke! [this test op]
    (assoc op :value
           (case (:f op)
             :fill-disk (c/on-nodes test (random-nonempty-subset (:nodes test))
                                    (fn [_ _] (fill! db)))
             :free-disk  (c/on-nodes test
                                     (fn [_ _] (free! db))))))

  (teardown! [this test])

  nem/Reflection
  (fs [this]
    #{:fill-disk :free-disk}))

(defn generator
  "Generates a random mixture of fill-disk and free-disk operations."
  [opts]
  (let [stop (fn [test _] {:type  :info
                           :f     :free-disk})
        start (fn [test _] {:type :info
                            :f    :fill-disk})]
    (->> (gen/mix [start stop])
         (gen/stagger (:interval opts)))))

(defn package
  "Options:

    :faults  A set of faults we expect to generate. Should include :disk
    :disk    Options specifically for disk failures

  Disk options are:

    :dir      The directory we'd like to turn into a tmpfs and fill
    :size-mb  The size, in megabytes, of that directory

  If faults includes disk, constructs a map with a DB, nemesis, generator, and
  perf descriptor suitable for testing tmpfs."
  [opts]
  (assert (set? (:faults opts)))
  (when ((:faults opts) :disk)
    (let [disk-opts (:disk opts)
          dir       (:dir disk-opts)
          size-mb   (:size-mb disk-opts)
          _         (assert (string? dir))
          _         (assert (pos? size-mb))
          db        (DB. (:dir disk-opts) (:size-mb disk-opts))]
      {:db              db
       :nemesis         (Nemesis. db)
       :generator       (generator opts)
       :final-generator {:type :info, :f :free-disk, :value nil}
       :perf            #{{:name  "disk"
                           :start #{:fill-disk}
                           :stop  #{:free-disk}
                           :color "#99DC58"}}})))
