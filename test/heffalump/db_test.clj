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

(def column-generators {
  :text gen/string
  :int gen/nat})

(defn gens-from-table-specs
  [table-specs id-col]
  (reduce merge
    (for [[tablename cols] table-specs]
      (let [cols (filter #(not (= % id-col)) cols)]
        {tablename (apply gen/hash-map (apply concat (for [[k t] cols] [k (get column-generators t)])))}))))

(def table-gens
  (gens-from-table-specs d/table-specs d/id-column))

(def gen-login
  (gen/hash-map
    :username gen/string
    :password gen/string))

(defn test-id-roundtrip
  [db tablename]
  (prop/for-all [row (get table-gens tablename)]
    (let [insert-result (du/insert-row! db tablename row)
          id (:id insert-result)
          test-fetch (du/get-by-id db tablename id)]
      (= insert-result test-fetch))))

(def global-test-db (new-test-db))

(defspec insert-and-get-accounts
  10
  (prop/for-all [[account bad-password] (gen/tuple gen-login gen/string)]
    (let [db global-test-db]
      (let [result (d/create-local-account! db account)
            auth-token (:auth-token result)
            refetch (du/get-by-id db :accounts (:id result))
            by-auth-token (d/token-user db auth-token)] 
        (and
          (= (:username refetch) (:username account))
          (= (:username by-auth-token) (:username account))
          (du/check-password (:password_hash refetch) (:password account))
          (or (= (:password account) bad-password)
              (not (du/check-password (:password_hash refetch) bad-password))))))))

(defspec test-status-roundtrip
  100
  (test-id-roundtrip global-test-db :statuses))

(defspec test-media-attr-roundtrip
  100
  (test-id-roundtrip global-test-db :media_attributes))

(defspec test-blocks-roundtrip
  100
  (test-id-roundtrip global-test-db :account_blocks))

(defspec test-mutes-roundtrip
  100
  (test-id-roundtrip global-test-db :account_mutes))

