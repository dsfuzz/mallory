; Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

(ns ^{:doc "Subprocess library"
      :author "Miki Tebeka <miki.tebeka@gmail.com>"}
 popen
  (:require [clojure.java.io :as io]))

(defn popen
  "Open a sub process, return the subprocess
args - List of command line arguments
:redirect - Redirect stderr to stdout
:dir - Set initial directory
:env - Set environment variables"
  [args & {:keys [redirect dir env]}]
  (let [pb (ProcessBuilder. args)
        environment (.environment pb)]
    (doseq [[k v] env] (.put environment k v))
    (-> pb
        (.directory (if (nil? dir) nil (io/file dir)))
        (.redirectErrorStream (boolean redirect))
        (.start))))

(defprotocol Popen
  (stdout [this] "Process standard output (read from)")
  (stdin [this] "Process standard input (write to)")
  (stderr [this] "Process standard error (read from)")
  (join [this] "Wait for process to terminate, return exit code")
  (exit-code [this] "Process exit code (will wait for termination)")
  (running? [this] "Return true if process still running")
  (kill [this] "Kill process"))

(defn- exit-code- [p]
  (try
    (.exitValue p)
    (catch IllegalThreadStateException e)))

(extend-type java.lang.Process
  Popen
  (stdout [this] (io/reader (.getInputStream this)))
  (stdin [this] (io/writer (.getOutputStream this)))
  (stderr [this] (io/reader (.getErrorStream this)))
  (join [this] (.waitFor this))
  (exit-code [this] (join this) (exit-code- this))
  (running? [this] (nil? (exit-code- this)))
  (kill [this] (.destroy this)))

(defn popen*
  "Open a sub process, return the subprocess stdout
args - List of command line arguments
:redirect - Redirect stderr to stdout
:dir - Set initial directory"
  [args & {:keys [redirect dir]}]
  (stdout (popen args :redirect redirect :dir dir)))
