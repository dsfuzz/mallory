(defproject jepsen.redis "0.1.0"
  :description "Jepsen tests for Redis"
  :url "https://github.com/jepsen-io/redis"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [jepsen "0.2.7-PASSIVE"]
                 [com.taoensso/carmine "2.19.1"]]
  :jvm-opts ["-Xmx4g" "-Djava.awt.headless=true"]
  :repl-options {:init-ns jepsen.redis.core}
  :main jepsen.redis.core)
