(ns heffalump.db-test
  (:import IdList)
  (:require [clojure.test :refer :all]
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

(deftest thread-id-sequence
  (testing "db thread id sequence"
    (let [db (new-test-db)]
      (is (= (d/new-thread-id! db) 0))
      (is (= (d/new-thread-id! db) 1)))))

(defn bounded-string-gen
  [length]
  (gen/fmap #(apply str %) 
            (gen/vector gen/char-alpha length)))

(def column-generators {
  :text (bounded-string-gen 4096)
  :clob gen/string
  "VARCHAR(200)" (bounded-string-gen 200)
  "VARCHAR(64)" (bounded-string-gen 64)
  :boolean gen/boolean
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

(defn map=
  [a b]
  (du/all?
    (for [[k v] a]
      (= v (get b k)))))

(defn test-id-roundtrip
  [db tablename]
  (prop/for-all [row (get table-gens tablename)]
    (let [insert-result (du/insert-row! db tablename row)
          id (:id insert-result)
          test-fetch (du/get-by-id db tablename id)]
      (map= insert-result test-fetch))))

(def global-test-db (new-test-db))

(defspec insert-and-get-accounts
  10
  (prop/for-all [[account bad-password] (gen/tuple gen-login gen/string)]
    (let [db @global-test-db]
      (let [result (d/create-local-account! db account)
            auth-token (:auth_token result)
            refetch (d/get-account db (:id result))
            by-auth-token (d/token-user db auth-token)] 
        (and
          (= (:username refetch) (:username account))
          (= (:username by-auth-token) (:username account))
          (or (= (:password account) bad-password)
            (du/check-password (:password_hash refetch) (:password account)))
          (or (= (:password account) bad-password)
              (not (du/check-password (:password_hash refetch) bad-password))))))))

(quote
(deftest tables-roundtrip
  (testing "tables roundtrip"
    (doseq [[table & _] d/table-specs]
      (prn table)
      (is
        (tc/quick-check 10
          (test-id-roundtrip global-test-db table)))))))

(defspec test-id-list-construct
  10000
  (prop/for-all [[start add size] (gen/tuple (gen/list gen/nat) (gen/list gen/nat) gen/nat)]
    (if (or (< 1 size) (<= (dec size) (count start)))
      true
      (let [lst (IdList. size start)]
        (and
          (= (seq lst) start)
          (or
            (not (seq add))
            (second
              (reduce
                (fn [[target ok?] item]
                  (.put lst item)
                  (let [new-target (take size (concat target (list item)))
                        ok? (and ok? (= new-target (take (count new-target) (seq lst))))]
                    (if (not ok?) 
                      (prn [new-target (seq lst) item]))
                    [new-target ok?]))
                [start true]
                add))))))))
