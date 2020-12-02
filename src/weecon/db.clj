(ns weecon.db
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as jdbc.sql]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]))

(Class/forName "org.sqlite.JDBC") ; ensure the sqlite JDBC driver is loaded.

(defn create-db! []
  (let [sqlite-filename (-> (java.io.File/createTempFile "weecon" ".db") .getAbsolutePath)
        ds              (->> sqlite-filename
                             (str "jdbc:sqlite:")
                             jdbc/get-datasource)]
    {:filename   sqlite-filename
     :ds         ds
     :connection (jdbc/get-connection ds)}))

(defn destroy-db! [{conn :connection path :filename}]
  (.close conn)
  (-> path clojure.java.io/file .delete))

(defn- columns->sql-list [columns column_format_str]
  (->> (map #(format column_format_str %) columns)
       (string/join ",")))

(defn create-tables! [{conn :connection}
                      {key-columns :key-columns
                       value-columns :value-columns
                       :as reconciliation-spec}]
  (let [create-table-sql-pattern (str "create table %s ("
                                      (string/join ", "
                                                   (flatten [(for [column-name key-columns]
                                                               (str column-name " TEXT NOT NULL"))
                                                             (for [column-name value-columns]
                                                               (str column-name " TEXT NULL"))
                                                             (str "PRIMARY KEY (" (string/join "," key-columns) ")")]))
                                      ")")
        recon-table-sql          (str "create table reconciliation (weecon_action TEXT NOT NULL,"
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
      (jdbc/execute! conn))
    (->> "test"
      (format create-table-sql-pattern)
      (conj [])
      (jdbc/execute! conn))
    (jdbc/execute! conn [recon-table-sql])))

(defmulti import! (fn [db table-name data-reader-spec] (:weecon.core/type data-reader-spec)))

(defmethod import! :default [db table-name {ds-type :weecon.core/type}]
  (throw (Exception. (str "Unknown data source type " ds-type))))

(defmethod import! "csv" [{conn :connection} table-name {file-name :file-name column-names :column-names separator :separator quote-char :quote-char}]
  (let [reader              (io/reader file-name)
        [header & contents] (csv/read-csv reader :separator (or separator \,) :quote (or quote-char \"))
        table               (keyword table-name)
        columns             (mapv keyword header)]
    (jdbc.sql/insert-multi! conn table columns contents)))

(defn reconcile!
  "I would normally perform the whole operation in one full outer join.  Since I have chosen sqlite
   that is sadly not possible.  Instead I simulate a full outer join in three statements as per
   https://www.sqlitetutorial.net/sqlite-full-outer-join/

  This function inserts rows into the table named reconciliation as a side effect."
  [{conn :connection}
   {key-columns :key-columns value-columns :value-columns :as reconciliation-spec}]
  (let [key-columns-sql-no-prefix    (columns->sql-list key-columns "%s")
        key-columns-sql-a-prefix     (columns->sql-list key-columns "a.%s")
        key-columns-sql-b-prefix     (columns->sql-list key-columns "b.%s")
        value-columns-sql-a-prefix   (columns->sql-list value-columns "a.%s")
        value-columns-sql-b-prefix   (columns->sql-list value-columns "b.%s")
        additions-sql                (str "INSERT INTO reconciliation (weecon_action,"
                                          key-columns-sql-no-prefix
                                          (when (seq value-columns) ",")
                                          (columns->sql-list value-columns "test_%s")
                                          ") SELECT 'added' as weecon_action, "
                                          key-columns-sql-a-prefix
                                          ","
                                          (columns->sql-list value-columns "a.%s")
                                          " FROM test a LEFT OUTER JOIN authority b USING ("
                                          key-columns-sql-no-prefix
                                          ") WHERE b."
                                          (first key-columns)
                                          " IS NULL")
        deletions-sql                (str "INSERT INTO reconciliation (weecon_action,"
                                          key-columns-sql-no-prefix
                                          (when (seq value-columns) ",")
                                          (columns->sql-list value-columns "authority_%s")
                                          ") SELECT 'deleted' as weecon_action, "
                                          key-columns-sql-a-prefix
                                          ", "
                                          value-columns-sql-a-prefix
                                          " FROM authority a LEFT OUTER JOIN test b USING ("
                                          key-columns-sql-no-prefix
                                          ") WHERE b."
                                          (first key-columns)
                                          " IS NULL")
        changes-sql                  (when (seq value-columns)  ; if there are no value columns then there can, by definition, be no changes.  Only additions or deletions.
                                       (str "INSERT INTO reconciliation (weecon_action,"
                                            key-columns-sql-no-prefix
                                            ","
                                            (columns->sql-list value-columns "authority_%s")
                                            ","
                                            (columns->sql-list value-columns "test_%s")
                                            ") SELECT 'changed' as weecon_action, "
                                            key-columns-sql-b-prefix
                                            ","
                                            (columns->sql-list value-columns "a.%1$s as authority_%1$s")
                                            ","
                                            (columns->sql-list value-columns "b.%1$s as test_%1$s")
                                            " FROM authority a JOIN test b USING ("
                                            key-columns-sql-no-prefix
                                            ") WHERE "
                                            (string/join " OR "
                                                         (map #(format "a.%1$s <> b.%1$s" %) value-columns))))]
    (doseq [stmt (remove nil? [additions-sql deletions-sql changes-sql])]
      (jdbc/execute! conn [stmt]))))

(defn report [{conn :connection path :filename}]
  (let [break-rows (jdbc/execute! conn ["SELECT weecon_action AS \"break-type\", count(*) AS \"count\" FROM reconciliation GROUP BY weecon_action ORDER BY weecon_action"])
        gzip-path  (str path ".gz")]
    (when (seq break-rows)
      (with-open [in (io/input-stream path)
                  gzip-out (-> gzip-path io/output-stream java.util.zip.GZIPOutputStream.)]
        (io/copy in gzip-out))
      {:filename    gzip-path
       :break-rows  break-rows})))

(comment
  (def aaa {:authority     {:file-name "config-examples/authority.csv" :weecon.core/type "csv" :column-names "header row"}
            :test          {:file-name "config-examples/test.csv" :weecon.core/type "csv" :column-names "header row"}
            :key-columns   ["name" "id_number"]
            :value-columns ["age" "height"]})
  (def db (create-db!))
  (create-tables! db aaa)
  (import! db "authority" (:authority aaa))
  (import! db "test" (:test aaa))
  (columns->sql-list ["name" "id_number"] "b.%s")
  (reconcile! db aaa)
  (destroy-db! db))
