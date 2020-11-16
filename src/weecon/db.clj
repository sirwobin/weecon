(ns weecon.db
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as jdbc.sql]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]))

(Class/forName "org.sqlite.JDBC") ; ensure the sqlite JDBC driver is loaded.

(defonce ds (str "jdbc:sqlite:" (-> (java.io.File/createTempFile "weecon" ".db") .getAbsolutePath)))

(defn- columns->sql-list [columns column_format_str]
  (->> (map #(format column_format_str %) columns)
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
                                                               [(str "authority_" column-name " TEXT NULL")
                                                                (str "test_" column-name " TEXT NULL")])
                                                             (str "PRIMARY KEY (" (columns->sql-list key-columns "%s") ")")]))
                                      ")")]
    (->> "authority"
      (format create-table-sql-pattern)
      (conj [])
      (jdbc/execute! ds))
    (->> "test"
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
  (let [key-columns-sql-no-prefix    (columns->sql-list key-columns "%s")
        key-columns-sql-b-prefix     (columns->sql-list key-columns "b.%s")
        value-columns-sql-b-prefix   (columns->sql-list value-columns "b.%s")
        additions-sql                (str "INSERT INTO reconciliation ("
                                          key-columns-sql-no-prefix
                                          (when (seq value-columns) ",")
                                          (columns->sql-list value-columns "test_%s")
                                          ") SELECT 'added' as weecon_action, "
                                          key-columns-sql-b-prefix
                                          ", "
                                          value-columns-sql-b-prefix
                                          " FROM test a LEFT OUTER JOIN authority b USING ("
                                          key-columns-sql-no-prefix
                                          ") WHERE b."
                                          (first key-columns)
                                          " IS NULL")
        deletions-sql                (str "INSERT INTO reconciliation ("
                                          key-columns-sql-no-prefix
                                          (when (seq value-columns) ",")
                                          (columns->sql-list value-columns "authority_%s")
                                          ") SELECT 'deleted' as weecon_action, "
                                          key-columns-sql-b-prefix
                                          ", "
                                          value-columns-sql-b-prefix
                                          " FROM authority a LEFT OUTER JOIN test b USING ("
                                          key-columns-sql-no-prefix
                                          ") WHERE b."
                                          (first key-columns)
                                          " IS NULL")
        changes-sql                  (str "INSERT INTO reconciliation ("
                                          key-columns-sql-no-prefix
                                          (when (seq value-columns) ",")
                                          (columns->sql-list value-columns "authority_%s")
                                          (when (seq value-columns) ",")
                                          (columns->sql-list value-columns "test_%s")
                                          ") SELECT 'changed' as weecon_action, "
                                          key-columns-sql-b-prefix
                                          (when (seq value-columns) ",")
                                          (columns->sql-list value-columns "a.%1$s as authority_%1$s")
                                          (when (seq value-columns) ",")
                                          (columns->sql-list value-columns "b.%1$s as test_%1$s"))])
  (jdbc.sql/query ds))

(comment
  (ns-unmap (find-ns 'weecon.db) 'ds)
  "
SELECT 'changed' as weecon_action, a.name, a.id_number,
       a.age as authority_age, b.age as test_age, a.height as authority_height, b.height as test_height
FROM   authority a
  JOIN test b USING (name, id_number)
WHERE  a.age <> b.age
   OR  a.height <> b.height
  "
  "
SELECT 'added' as weecon_action, b.name, b.id_number,
       b.age, b.height
FROM   test a
  LEFT OUTER JOIN authority b USING (name, id_number)
WHERE  b.name IS NULL
  "
  (create-tables {:authority     {:file-name "config-examples/authority.csv" :file-type "csv" :column-names "header row"}
                  :test          {:file-name "config-examples/test.csv" :file-type "csv" :column-names "header row"}
                  :key-columns   ["name" "id_number"]
                  :value-columns ["age" "height"]})
  (import-csv "config-examples/authority.csv" "authority")
  (import-csv "config-examples/test.csv" "test")
  (columns->sql-list ["name" "id_number"] "b.%s"))
