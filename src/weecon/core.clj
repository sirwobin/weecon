(ns weecon.core
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [schema.core :as schema]
            [schema.utils]
            [weecon.db]
            [weecon.mail])
  (:gen-class))

(def data-reader-schema {:file-name         schema/Str
                         :weecon.core/type  (schema/enum "csv")
                         :column-names      (schema/either schema/Str [schema/Str])})

(def reconciliation-spec-schema {:authority     data-reader-schema
                                 :test          data-reader-schema
                                 :key-columns   [schema/Str]
                                 :value-columns [schema/Str]
                                 :outputs       [weecon.mail/mail-config-spec-schema]})

(defn load-data-reconcile-send-results! [{authority-reader-spec :authority test-reader-spec :test :as reconciliation-spec}]
  (try
    (weecon.db/create-tables! reconciliation-spec)
    (weecon.db/import! "authority" authority-reader-spec)
    (weecon.db/import! "test" test-reader-spec)
    (weecon.db/reconcile! reconciliation-spec)

    (catch java.io.FileNotFoundException X
      (->> X .getMessage (println "error:")))
    (catch Exception X
      (println "while attempting the reconciliation: " X))))

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
      (println "The reconciliaton specified in file" config-file "is not correct:\n" (pr-str schema-check))
      (load-data-reconcile-send-results! reconciliation-spec))))
