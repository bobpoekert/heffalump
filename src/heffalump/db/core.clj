(ns heffalump.db.core
  (:require [heffalump.db-utils :refer :all]
            [manifold.deferred :as d]
            [clojure.data.fressian :as fress]
            [byte-streams :as bs]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.io :as io]))

(def tables (atom []))

(defn add-table!
  [tablename columns]
  (swap! tables
    (fn [tables]
      (conj tables [tablename columns]))))

(def indexes (atom []))

(defn add-index!
  [table col]
  (swap! indexes
    (fn [indexes]
      (conj indexes [table col]))))

(def ddl-queries (atom []))

(defn add-ddl-query!
  [q]
  (swap! ddl-queries #(conj % q)))

(def id-column [:id "INTEGER NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 1, increment by 1)"])

(add-table! :dump
  [
    [:entity :int]
    [:k :text]
    [:v :text]])

(defquery get-dump-query
  [k entity] [:v]
  "select v from dump where k = ? and entity = ?"
  (partial mapv :v))

(defn get-dump
  ([db k] (get-dump db k nil))
  ([db k entity]
    (d/let-flow [result-string (get-dump-query db k entity)]
      (if result-string
        (fress/read (bs/convert result-string java.io.Reader))))))

(defn put-dump!
  ([db k v] (put-dump! db nil k v))
  ([db entity k v]
    (insert-row! db :dump {
      :entity entity
      :k k
      :v (fress/write v)})))

(add-ddl-query! "create sequence thread_id_sequence start with 0")
(add-ddl-query! "create sequence clock_sequence start with 0")

(defquery new-clock!
  [] [:v]
  "VALUES NEXT VALUE FOR clock_sequence"
  #(:v (first %)))

(defn setup-db!
  [db]
  (let [db (maybe-deref db)]
    (create-tables! db @tables)
    (create-indexes! db @indexes)
    (doseq [q @ddl-queries]
      (jdbc/execute! db q))))

(defn init!
  [config]
  (let [db-setup? (atom false)
        cache (create-cache)
        pathname (:subname (:db config))
        new-db-file (or (.startsWith ^String pathname "memory:")
                       (not (.exists (io/file pathname))))]
    (thread-local
      (let [db (create-db config)]
        (when (and (not @db-setup?)
                   (:init-db config)
                   new-db-file)
          (swap! db-setup? (fn [v] true))
          (setup-db! db))
        (assoc
          (populate-queries db @tables)
          :cache cache)))))
