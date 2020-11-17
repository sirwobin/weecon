(ns weecon.core
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [schema.core :as schema]
            [schema.utils])
  (:gen-class))

(def data-file-schema {:file-name schema/Str
                       :file-type schema/Str
                       :column-names (schema/either schema/Str [schema/Str])})

(def reconciliation-spec-schema {:authority     data-file-schema
                                 :test          data-file-schema
                                 :key-columns   [schema/Str]
                                 :value-columns [schema/Str]})

(defn -main
  [& [config-file & more-args]]
  (when-not config-file
    (log/error "usage: weecon <config-file>")
    (System/exit 1))
  (when-not (-> config-file io/file .exists)
    (log/errorf "file %s does not exist." config-file)
    (System/exit 2))
  (let [reconciliation-spec   (-> config-file
                                  io/reader
                                  (json/read :key-fn keyword))
        schema-check          (schema/check reconciliation-spec-schema reconciliation-spec)]
    (if (seq schema-check)
      (do
        (println "The reconciliaton specified in file" config-file "is not correct:")
        (println (pr-str schema-check)))
      (println "TODO will call reconcile! with config" (pr-str reconciliation-spec)))))
