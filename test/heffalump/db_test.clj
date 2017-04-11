(ns heffalump.db-test
  (require [clojure.test :refer :all]
           [clojure.test.check :as tc]
           [clojure.test.check.generators :as gen]
           [clojure.test.check.properties :as prop]
           [heffalump.db :as d]
           [heffalump.db-utils :as du]))

(defn test-config
  []
  {
    :data-dir nil
    :db {
      :classname "org.apache.derby.jdbc.EmbeddedDriver"
      :subprotocol "derby"
      :subname (str "memory:" (du/b64encode (du/random-number 16)))
      :create true}
    :init-db true
    :port 8000
    :hostname "localhost"})

(defn new-test-db
  []
  (d/init! (test-config)))

(defmacro with-test-db
  [dbname & body]
  `(let [~dbname @(new-test-db)]
    (with-open [^java.sql.Connection dbc# (:connection ~dbname)]
      ~@body)))

(testing "db thread id sequence"
  (with-test-db db
    (is (= (d/new-thread-id db) 0))
    (is (= (d/new-thread-id db) 1))))
