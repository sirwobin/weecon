(ns weecon.db
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as jdbc.sql]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]))

(Class/forName "org.sqlite.JDBC") ; ensure the sqlite JDBC driver is loaded.

(defonce ds (str "jdbc:sqlite:" (-> (java.io.File/createTempFile "weecon" ".db") .getAbsolutePath)))

(defn- columns->sql-list [columns table-prefix]
  (->> (map #(str table-prefix %) columns)
       (string/join ", ")))

(defn create-tables [{key-columns :key-columns value-columns :value-columns :as reconciliation-spec}]
  (let [create-table-sql-pattern (str "create table %s ("
                                      (string/join ", "
                                                   (flatten [(for [column-name key-columns]
                                                               (str column-name " TEXT NOT NULL"))
                                                             (for [column-name value-columns]
                                                               (str column-name " TEXT NULL"))
                                                             (str "PRIMARY KEY (" (string/join "," key-columns) ")")]))
                                      ")")
        recon-table-sql          (str "create table reconciliation ("
                                      (string/join ", "
                                                   (flatten [(for [column-name key-columns]
                                                               (str column-name " TEXT NOT NULL"))
                                                             (for [column-name value-columns]
                                                               [(str "source_" column-name " TEXT NULL")
                                                                (str "destination_" column-name " TEXT NULL")])
                                                             (str "PRIMARY KEY (" (columns->sql-list key-columns "") ")")]))
                                      ")")]
    (->> "source"
      (format create-table-sql-pattern)
      (conj [])
      (jdbc/execute! ds))
    (->> "destination"
      (format create-table-sql-pattern)
      (conj [])
      (jdbc/execute! ds))
    (jdbc/execute! ds [recon-table-sql])))

(defn import-csv [file-name table-name & [separator quote]]
  (let [reader              (io/reader file-name)
        [header & contents] (csv/read-csv reader :separator (or separator \,) :quote (or quote \"))
        table               (keyword table-name)
        columns             (mapv keyword header)]
    (jdbc.sql/insert-multi! ds table columns contents)))

(defn reconciliation [{key-columns :key-columns value-columns :value-columns :as reconciliation-spec}]
  (let [additions-sql (str "SELECT 'added' as weecon_action, "
                           (columns->sql-list key-columns "b.")
                           ", "
                           (columns->sql-list value-columns "b.")
                           " FROM destination a LEFT OUTER JOIN source b USING ("
                           (columns->sql-list key-columns "")
                           ") WHERE b."
                           (first key-columns))
        deletions-sql (str "SELECT 'deleted' as weecon_action, "
                           (columns->sql-list key-columns "b.")
                           ", "
                           (columns->sql-list value-columns "b.")
                           " FROM source a LEFT OUTER JOIN destination b USING ("
                           (columns->sql-list key-columns "")
                           ") WHERE b."
                           (first key-columns))])
  (jdbc.sql/query ds))

(comment
  (ns-unmap (find-ns 'weecon.db) 'ds)
  "
SELECT 'changed' as weecon_action, a.name, a.id_number,
       a.age as source_age, b.age as destination_age, a.height as source_height, b.height as destination_height
FROM   source a
  JOIN destination b USING (name, id_number)
WHERE  a.age <> b.age
   OR  a.height <> b.height
  "
  "
SELECT 'added' as weecon_action, b.name, b.id_number,
       b.age, b.height
FROM   destination a
  LEFT OUTER JOIN source b USING (name, id_number)
WHERE  b.name IS NULL
  "
  (create-tables {:source        {:file-name "config-examples/source.csv" :file-type "csv" :column-names "header row"}
                  :destination   {:file-name "config-examples/destination.csv" :file-type "csv" :column-names "header row"}
                  :key-columns   ["name" "id_number"]
                  :value-columns ["age" "height"]})
  (import-csv "config-examples/source.csv" "source")
  (import-csv "config-examples/destination.csv" "destination")
  (columns->sql-list ["name" "id_number"] "b."))
