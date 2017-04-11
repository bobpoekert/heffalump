(ns heffalump.db-utils
  (import [com.google.common.cache CacheBuilder]
          [javax.crypto SecretKey SecretKeyFactory]
          [javax.crypto.spec PBEKeySpec]
          [java.security SecureRandom]
          [org.mindrot.jbcrypt BCrypt]
          ThreadLocalThing)
  (require [clojure.string :as s]
           [clojure.java.io :as io]
           [clojure.java.jdbc :as jdbc]
           [clojure.data.fressian :as fress]
           [byte-streams :as bs]))

(defn make-thread-local
  [generator]
  (ThreadLocalThing. ::initial-val generator))

(defmacro thread-local
  [& body]
  `(make-thread-local (fn [] ~@body)))

(defn hash-password
  [password-string]
  (BCrypt/hashpw password-string (BCrypt/gensalt)))

(defn check-password
  [password-hash input]
  (BCrypt/checkpw input password-hash))

(defn create-tables!
  [db tables]
  (doseq [[tablename columns] tables]
    (let [columns (for [[l r] columns] [l (if (= r :text) "VARCHAR(4096)" r)])
          query (jdbc/create-table-ddl tablename columns)]
      (jdbc/execute! db query))))

(defn create-index!
  [db [table-name column-name]]
  (let [sql-table-name (name table-name)
        sql-column-name (name column-name)]
    (jdbc/execute! db
      (format "create index %s on %s (%s)" 
        (format "index_%s_%s" sql-table-name sql-column-name)
        sql-table-name
        sql-column-name))))

(defn create-indexes!
  [db indexes]
  (doseq [index indexes]
    (create-index! db index)))

(defn create-fk-constraint!
  [db [[left-table left-column] [right-table right-column]]]
  (let [sql-left-table (name left-table)
        sql-left-column (name left-column)
        sql-right-table (name right-table)
        sql-right-column (name right-column)
        query   
          (format "ALTER TABLE %s ADD CONSTRAINT fk_%s_%s_%s_%s FOREIGN KEY (%s) REFERENCES %s (%s)"
            sql-left-table
            sql-left-table sql-left-column sql-right-table sql-right-column
            sql-left-column
            sql-right-table sql-right-column)]
    (println query)
    (jdbc/execute! db query)))

(defn create-fk-constraints!
  [db fks] 
  (doseq [fk fks]
    (create-fk-constraint! db fk)))

(defn query-one
  [db query]
  (jdbc/query @db query {:result-set-fn first}))

(defn random-number
  [size]
  (->
    (SecureRandom/getInstance "SHA1PRNG")
    (.generateSeed size)))

(defn b64encode
  [ins]
  (let [encoder (java.util.Base64/getEncoder)]
    (.encodeToString encoder (bs/to-byte-array ins))))

(defn create-cache
  []
  (->
    (CacheBuilder/newBuilder)
    (.softValues)
    (.build)
    (.asMap)))

(defmacro cached
  [db cache-key value-generator]
  `(let [cache-key# ~cache-key
         ^java.util.Map cache# (:cache ~db)
         cache-value# (get cache# cache-key#)]
    (if cache-value#
      cache-value#
      (let [result# ~value-generator]
        (if-not (nil? result#)
          (.put cache# cache-key# cache-value#))
        cache-value#))))

(defn statement-set-params!
  [^java.sql.PreparedStatement stmt params]
  (loop [[param & rst] params
         idx 1]
    (.setObject stmt idx (jdbc/sql-value param))
    (if rst
      (recur rst (inc idx)))))

(defn run-prepared-query
  [db ^java.sql.PreparedStatement stmt args result-set-fn]
  (statement-set-params! stmt args)
  (with-open [results (.executeQuery stmt)]
    (result-set-fn (jdbc/result-set-seq results)))) 

(defn generate-by-id-query
  [db tablename]
  (let [conn (:connection db)
        sql-string (format "select * from %s where id = ?" (name tablename))
        _ (println sql-string)
        statement (jdbc/prepare-statement conn sql-string)]
    {tablename statement}))

(defn get-by-id-query
  [db tablename]
  (get (:by-id-queries db) tablename)) 

(defn get-by-id
  [db table id]
  (cached db [:by-id table id]  
    (let [by-id-query (get-by-id-query db table)]
      (run-prepared-query db by-id-query [id] first))))

(defn update-row!
  [db table-name row]
  (let [id (:id row)
        where-clause ["id = ?" id]
        res (jdbc/update! db table-name row where-clause)]
    (.remove ^java.util.Map (:cache db) [:by-id table-name id])
    res))

(defn populate-queries
  [db tables]
  (assoc db
     :by-id-queries (reduce merge
                      (for [[tablename specs] tables]
                        (if (some #(= (name (first %)) "id") specs)
                          (generate-by-id-query db tablename)
                          {})))))

(defn create-db
  [config]
  (let [cache (create-cache)]
    {:cache cache
     :connection (jdbc/get-connection (:db config))}))
    
