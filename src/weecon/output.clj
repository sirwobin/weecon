(ns weecon.output)

; additional methods are defined in other name spaces.
(defmulti send! (fn [output-spec] (:weecon.output/type output-spec)))

(defmethod send! :default [{output-type :weecon.output/type :as output-spec}]
  (throw (Exception. (str "Unknown output type " output-type))))

