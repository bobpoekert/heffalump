(ns heffalump.config
  (require [clojure.java.io :as io])
  (use korma.db))

(def default-config
  (let [data-dir "~/.heffalump"]
    {
      :data-dir data-dir
      :db {
        :classname "org.apache.derby.jdbc.EmbeddedDriver"
        :subprotocol "derby"
        :subbname (str data-dir "/db.derby")
        :create true}
      :port 8000
      :hostname "heffalump.zone"}))

(defn load-config
  [fname]
  (with-open [fd (io/reader fname)]
    (eval (read-string (slurp fd)))))
