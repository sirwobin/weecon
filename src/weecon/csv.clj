(ns weecon.csv
  (:require [clojure.data.csv :as csv]))

(defn read-csv
  "Given a reader and optional separete/quotes will read in a csv and return a list of row maps."
  [reader & {:keys [separator quote] :as opts}]
  (let [[header & contents] (csv/read-csv reader :separator (or separator \,) :quote (or quote \"))
        header-count (count header)]
    (map-indexed (fn [idx line]
                   (with-meta (zipmap header (minimum-length header-count line))
                              {:address (inc idx)}))
                 contents)))
