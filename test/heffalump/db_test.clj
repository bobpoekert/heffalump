(ns heffalump.db-test
  (require [clojure.test :refer :all]
           [clojure.test.check :as tc]
           [clojure.test.check.generators :as gen]
           [clojure.test.check.properties :as prop]
           [heffalump.db :as d]
           [heffalump.db-utils :as du]))

(def test-config
  {
    :data-dir nil
    :db {
      :classname "org.apache.derby.jdbc.EmbeddedDriver"
      :subprotocol "derby"
      :subname (str "memory:" (du/b64encode (du/random-number 8)))
      :create true}
    :init-db true
    :port 8000
    :hostname "localhost"})

(defn new-test-db
  []
  (d/init! test-config))

(defmacro with-test-db
  [dbname & body]
  `(let [~dbname @(new-test-db)]
    (with-open [^java.sql.Connection dbc# (:connection ~dbname)]
      ~@body)))

(testing "db init"
  (with-test-db db
    (du/get-by-id db :accounts 0)))
