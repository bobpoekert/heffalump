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

(set! *warn-on-reflection* true)

(defprotocol MaybeDeref
  (maybe-deref [v]))

(extend-protocol MaybeDeref
  clojure.lang.IDeref
  (maybe-deref [v] (deref v))
  Object
  (maybe-deref [v] v))

(def db-create-actions (atom []))

(defn on-db-create!
  [thunk]
  (swap! db-create-actions (fn [old] (conj old thunk))))

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

(defn prepare-statement
  [db sql-string & args]
  (try
    (apply jdbc/prepare-statement (:connection db) sql-string args)
    (catch Exception e
      (throw (RuntimeException. (format "exception compiling \"%s\": %s" sql-string (str e)))))))

(defn create-tables!
  [db tables]
  (doseq [[tablename columns] tables]
    (let [columns (for [[l r] columns] [l (if (= r :text) "VARCHAR(4096)" r)])
          query (format "CREATE TABLE %s (%s)" (name tablename) (s/join ", " (map #(s/join " " (map name %)) columns)))]
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
    (jdbc/execute! db query)))

(defn create-fk-constraints!
  [db fks] 
  (doseq [fk fks]
    (create-fk-constraint! db fk)))

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
         ^java.util.Map cache# (:cache (maybe-deref ~db))
         cache-value# (get cache# cache-key#)]
    (if cache-value#
      cache-value#
      (let [result# ~value-generator]
        (if-not (nil? result#)
          (.put cache# cache-key# result#)
        result#)))))

(defn statement-set-params!
  [^java.sql.PreparedStatement stmt params]
  (if (seq params)
    (loop [[param & rst] params
           idx 1]
      (.setObject stmt idx (jdbc/sql-value param))
      (if rst
        (recur rst (inc idx))))))

(defn run-prepared-query
  [db ^java.sql.PreparedStatement stmt args result-set-fn]
  (statement-set-params! stmt args)
  (with-open [results (.executeQuery stmt)]
    (result-set-fn results))) 

(defn generate-by-id-query
  [db tablename col-specs]
  (let [col-names (for [col col-specs] (name (first col)))
        sql-string (format "select %s from %s where id = ?" (s/join ", " col-names) (name tablename))
        statement (prepare-statement db sql-string)
        typename (format "full-row-%s" (name tablename))
        type-constructor-sym (symbol (format "%s." typename))
        rs-sym (with-meta (gensym "rs") {:tag "java.sql.ResultSet"})
        row-reader (eval `(do
                                  (defrecord ~(symbol typename) ~(mapv symbol col-names))
                                  (fn [~rs-sym]
                                    ~(cons type-constructor-sym
                                      (for [idx (range (count col-names))] `(.getObject ~rs-sym ~(inc idx)))))))]
    {tablename [statement row-reader]}))

(defn prepared-query-generator
  [query-name query]
  (fn [db]
    {query-name (prepare-statement db query)})) 

(defn- gen-result-set-unpacker
  [return-keys record-sym]
  (let [rs (with-meta (gensym "rs") {:tag "java.sql.ResultSet"})]
    `(fn [seq-consumer#]
      (fn [~rs]
        (seq-consumer#
          (
            (fn seqer# []
              (when (.next ~rs)
                (cons
                  ~(cons
                      (symbol (format "->%s" (name record-sym)))
                      (for [idx (range (count return-keys))]
                       `(.getObject ~rs ~(inc idx))))
                  (lazy-seq (seqer#)))))))))))

(defn- defquery-fn
  ([query-name args return-keys query] (defquery-fn query-name args return-keys query 'doall))
  ([query-name args return-keys query result-fn]
    (let [db-sym (gensym "db")
          result-fn-sym (gensym "result-set-fn")
          name-kw (keyword query-name)
          record-name (gensym (name query-name))]
     `(do
        (defrecord ~record-name ~(mapv #(symbol (name %)) return-keys))
        (on-db-create! (prepared-query-generator ~name-kw ~query))
        (let [result-set-unpacker# ~(gen-result-set-unpacker return-keys record-name)] 
          (defn ~query-name
            (~(vec (concat [db-sym result-fn-sym] args))
              (let [db# (maybe-deref ~db-sym)
                    q# (get db# ~name-kw)]
                (run-prepared-query db# q# ~args (result-set-unpacker# ~result-fn-sym))))
            (~(vec (concat [db-sym] args))
              ~(concat (list query-name db-sym result-fn) args))))))))

(defmacro defquery
  [& args]
  (apply defquery-fn args))

(defquery last-row-id
  [] [:id]
  "VALUES identity_val_local()"
  #(:id (first %)))

(defn get-by-id-query
  [db tablename]
  (get (:by-id-queries db) tablename)) 

(defn table-spec-has-id?
  [specs]
  (not (nil? (some #(= (name (first %)) "id") specs))))

(defn generate-insert-query
  [db [tablename specs]]
  (let [cols (for [col specs] (first col))
        has-id-col (boolean (some #(= % :id) cols))
        cols (filter #(not (= % :id)) cols)
        col-names (map name cols)]
    {tablename [
      (prepare-statement db 
        (format "INSERT INTO %s (%s) VALUES (%s)"
          (name tablename)
          (s/join ", " col-names)
          (s/join ", " (repeat (count col-names) "?")))
        {:return-keys true})
      has-id-col cols]}))
    
(defn populate-statement!
  [db ^java.sql.PreparedStatement stmt column-keys row]
  (.clearParameters stmt)
  (loop [[kk & rst] column-keys
         idx 1]
    (when kk
      (.setObject stmt idx (jdbc/sql-value (get row kk)))
      (recur rst (inc idx)))))

(defn param-count
  [^java.sql.PreparedStatement stmt]
  (.getParameterCount (.getParameterMetaData stmt)))

(defn param-type-names
  [^java.sql.PreparedStatement stmt]
  (for [idx (range (param-count stmt))]
    (.getParameterClassName (.getParameterMetaData stmt) (inc idx))))

(defn insert-row!
  [db tablename row]
  (let [db (maybe-deref db)
        [^java.sql.PreparedStatement query has-id-col? cols] (get (:insert-queries db) tablename)]
    (populate-statement! db query cols row)
    (.executeUpdate query)
    (.commit ^java.sql.Connection (:connection db))
    (if has-id-col?
      (assoc row :id (last-row-id db))
      row)))
                
(defn get-by-id
  [db table id]
  (let [db (maybe-deref db)
        [by-id-query by-id-constructor] (get-by-id-query db table)]
    (run-prepared-query db by-id-query [id]
      (fn [^java.sql.ResultSet rs]
        (when (.next rs)
          (by-id-constructor rs))))))

(defn update-row!
  [db table-name row]
  (let [db (maybe-deref db)
        id (:id row)
        where-clause ["id = ?" id]
        res (jdbc/update! db table-name row where-clause)]
    (.commit ^java.sql.Connection (:connection db))
    res))

(defn populate-queries
  [db tables]
  (reduce (fn [coll thunk] (merge coll (thunk db)))
    (merge db {
       :tables tables
       :by-id-queries (reduce merge
                        (for [[tablename specs] tables]
                          (if (table-spec-has-id? specs) 
                            (generate-by-id-query db tablename specs)
                            {})))
      :insert-queries (reduce merge (map (partial generate-insert-query db) tables))})
    @db-create-actions))
                        
(defn create-db
  [config]
  (let [conn (jdbc/get-connection (:db config))]
    {:connection conn}))
    
