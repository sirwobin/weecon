(ns weecon.output)

; additional methods are defined in other name spaces.
(defmulti send! (fn [output-spec report] (:weecon.output/type output-spec)))

(defmethod send! :default [{output-type :weecon.output/type :as output-spec} _]
  (throw (Exception. (str "Unknown output type " output-type))))

(defn html-and-text [{break-rows :break-rows path :filename}]
  (let [{total        :total
         text-summary :text-str
         html-table   :html-str}  (reduce (fn [{total    :total
                                                text-str :text-str
                                                html-str :html-str}
                                               {break-type :reconciliation/break-type
                                                c          :count}]
                                            {:total    (+ total c)
                                             :text-str (format "%s%s: %d\n" text-str break-type c)
                                             :html-str (format "%s<tr><td>%s</td><td>%d</td></tr>\n" html-str break-type c)})
                                          {:total    0
                                           :text-str ""
                                           :html-str "<table><tr><th>Break Type</th><th>Count</th></tr>\n"}
                                          break-rows)]
    {:filename path
     :html     (format "<p>There are %d reconciliation breaks in total.</p><p>%s</table></p>" total html-table)
     :text     (format "There are %d reconciliation breaks in total.\n\n%s\n" total text-summary)}))

(comment
  (let [r [{:count 1 :reconciliation/break-type "added"}
           {:count 1 :reconciliation/break-type "changed"}
           {:count 1 :reconciliation/break-type "deleted"}]]
    (html-and-text r)))
