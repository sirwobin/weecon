(defproject weecon "0.1.0-SNAPSHOT"
  :description  "FIXME: write description"
  :url          "http://example.com/FIXME"
  :license      {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
                 :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure            "1.10.1"]
                 [org.clojure/data.json          "1.0.0"]
                 [org.clojure/data.csv           "1.0.0"]
                 [seancorfield/next.jdbc         "1.1.613"]
                 [org.xerial/sqlite-jdbc         "3.32.3.2"]
                 [prismatic/schema               "1.1.12"]
                 [com.sun.mail/javax.mail        "1.6.2"]
                 [org.clojure/tools.logging      "1.1.0"]
                 [org.slf4j/slf4j-api            "1.7.30"]
                 [ch.qos.logback/logback-classic "1.2.3"]]
  :repl-options {:init-ns weecon.core}
  :main         weecon.core
  :profiles     {:uberjar {:aot      :all
                           :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}
                 :dev     {:plugins [[lein-shell "0.5.0"]]}}
  :aliases      {"native" ["shell" "native-image"
                           "-H:+TraceClassInitialization" "--no-fallback"
                           "--report-unsupported-elements-at-runtime" "--initialize-at-build-time" "-jar"
                           "./target/${:uberjar-name:-${:name}-${:version}-standalone.jar}" "-H:Name=./target/weecon"]})
