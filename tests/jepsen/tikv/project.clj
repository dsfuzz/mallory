; Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

(defproject jepsen.tikv "0.1.0-SNAPSHOT"
  :description "Jepsen test for TiKV"
  :url "https://tikv.org"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main jepsen.tikv
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [jepsen "0.2.7-PASSIVE"]
                 [clj-http "3.10.0"]
                 ;; -- PROTOC-GEN-CLOJURE --
                 [protojure "1.5.11"]
                 [protojure/google.protobuf "0.9.1"]
                 [com.google.protobuf/protobuf-java "3.13.0"]
                 ;; -- PROTOC-GEN-CLOJURE HTTP/2 Client Lib Dependencies --
                 [org.eclipse.jetty.http2/http2-client "9.4.20.v20190813"]
                 [org.eclipse.jetty/jetty-alpn-java-client "9.4.28.v20200408"]
                 ;; -- Jetty Client Dep --
                 [org.ow2.asm/asm "8.0.1"]]
  :repl-options {:init-ns jepsen.tikv}
  :jvm-opts ["-Xmx4g" "-Djava.awt.headless=true"])
