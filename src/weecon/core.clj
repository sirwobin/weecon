(ns weecon.core
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.data.json :as json])
  (:gen-class))

(defn -main
  [& [config-file & more-args]]
  (when-not config-file
    (log/error "usage: weecon <config-file>")
    (System/exit 1))
  (when-not (-> config-file io/file .exists)
    (log/errorf "file %s does not exist." config-file)
    (System/exit 2))
  (let [reconciliation-config (-> config-file io/reader (json/read :key-fn keyword))]
    (println "config is" (pr-str reconciliation-config))))
