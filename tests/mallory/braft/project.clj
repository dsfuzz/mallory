(defproject jepsen.atomic "0.1.0-SNAPSHOT"
  :description "Jepsen test framework for Baidu Raft's implementation"
  :main jepsen.atomic
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [gnuplot "0.1.1"]
                 [clj-ssh "0.5.14"]
                 [jepsen "0.2.7-MEDIATOR-SNAPSHOT"]]
  :jvm-opts ["-server"
             ;"-XX:-OmitStackTraceInFastThrow"
             "-Djava.awt.headless=true"
             ; GC tuning--see
             ; https://wiki.openjdk.java.net/display/shenandoah/Main
             ; https://wiki.openjdk.java.net/display/zgc/Main
             ;"-XX+UseZGC"
             ;"-XX+UseShenandoahGC"
             "-Xmx24g"
             ;"-XX:+UseLargePages" ; requires users do so some OS-level config
             "-XX:+AlwaysPreTouch"
             ; Instrumentation
             ;"-agentpath:/home/aphyr/yourkit/bin/linux-x86-64/libyjpagent.so=disablestacktelemetry,exceptions=disable,delay=10000"
             ])

