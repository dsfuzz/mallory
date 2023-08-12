(defproject jepsen.dqlite "0.1.0"
  :description "Jepsen tests for Dqlite, a SQLite-based system with Raft consensus."
  :url "https://dqlite.io"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.2.7-MEDIATOR-SNAPSHOT"]
                 [clj-http "3.10.1"]]
  :main jepsen.dqlite
  :jvm-opts ["-Djava.awt.headless=true"
             "-server"
             ;"-XX:+PrintGCDetails"
             ;"-verbose:gc"
             ]
  :repl-options {:init-ns jepsen.dqlite})
