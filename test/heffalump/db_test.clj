(ns heffalump.db-test
  (require [clojure.test :refer :all]
           [clojure.test.check :as tc]
           [clojure.test.check.generators :as gen]
           [clojure.test.check.properties :as prop]
           [clojure.test.check.clojure-test :refer [defspec]]
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
    (is (= (d/new-thread-id! db) 0))
    (is (= (d/new-thread-id! db) 1))))

(def gen-account
  (gen/hash-map
    :username gen/string
    :password gen/string))

(def global-test-db (new-test-db))

(defspec insert-and-get-accounts
  10
  (prop/for-all [[account bad-password] (gen/tuple gen-account gen/string)]
    (let [db global-test-db]
      (let [result (d/create-local-account! db account)
            refetch (du/get-by-id db :accounts (:id result))]
        (and
          (= (:username refetch) (:username account))
          (du/check-password (:password_hash refetch) (:password account))
          (or (= (:password account) bad-password)
              (not (du/check-password (:password_hash refetch) bad-password))))))))
