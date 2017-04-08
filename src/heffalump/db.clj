(ns heffalump.db
  (import [com.google.common.cache CacheBuilder]
          [javax.crypto SecretKey SecretKeyFactory]
          [javax.crypto.spec PBEKeySpec]
          [java.security SecureRandom]
          [org.mindrot.jbcrypt BCrypt])
  (require [clojure.string :as s]
           [clojure.java.io :as io]
           [clojure.java.jdbc :as jdbc]
           [clojure.data.fressian :as fress]
           [byte-streams :as bs]))

(set! *warn-on-reflection* true)

(defn hash-password
  [password-string]
  (BCrypt/hashpw password-string (BCrypt/gensalt)))

(defn check-password
  [password-hash input]
  (BCrypt/checkpw input password-hash))

(def id-column [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"])

(def table-specs
  [
    [:dump
      [
        :entity :int
        :key :text
        :value :text]]
    [:statuses
      [
        id-column
        :uri :text ;fediverse-unique resource ID
        :url :text ;URL to the status page (can be remote)
        :account_id :int ; Account FK
        :in_reply_to_id :int ; null or ID of status it replies to
        ;; on status insert, if the status is in_reply_to another status,
        ;; this status has the same thread id. otherwise, thread_id = id
        :thread_id :int
        :thread_depth :int
        :reblog :int ; null or status
        :content :text ; Body of the status. Contains HTML
        :created_at :int ; UTC epoch timestamp of when this status was created
        ;; reblogs_count is a thing in the API response but that doesn't belong in the DB
        ;; likewise favourites_count
        :application_id :int ; which Application posted this
        :deleted_at :int
        ]]
    [:media_attributes
      [
        id-column
        :status_id :int
        :url :text ; image asset URL
        :preview_url :text ; image preview (ie: resized) asset url
        :type :int ; 0 for image and 1 for video
      ]]
    [:mentions
      [
        id-column
        :status_id :int
        :sender_id :int
        :recipient_id :int
        :created_at :int
        :deleted_at :int]]
    [:accounts
      [
      id-column
      :username :text
      :acct :text ; null for local users (use username), username@domain for remote
      :display_name :text
      :note :text ; biography of user
      :url :text ; profile page URL (can be remote)
      :avatar :text ; url of user's av image
      :header :text ; url of user's header image
      ;; followers_count, following_count, and statuses_count can be computed
      :password_hash :text
      :auth_token :text
      :deleted_at :int
      ]]
    [:account_blocks
      [
        id-column
        :blocker :int
        :blockee :int]]])
   
(def indexes
  [
    [:accounts :auth_token]
    [:mentions :status_id]
    [:mentions :recipient_id]
    [:media_attributes :status_id]
    [:statuses :thread_id]
    [:account_blocks :blocker]])

(def fk-constraints
  [
    [[:media_attributes :status_id] [:statuses :id]]
    [[:mentions :status_id] [:statuses :id]]
    [[:statuses :account_id] [:accounts :id]]])

(defn create-tables!
  [db tables]
  (doseq [[tablename columns] tables]
    (jdbc/query (:conn db) (jdbc/create-table-ddl tablename columns))))

(defn create-index!
  [db [table-name column-name]]
  (let [sql-table-name (jdbc/as-sql-name (name table-name))
        sql-column-name (jdbc/as-sql-name (name column-name))]
    (jdbc/query db
      (format "create index %s on %s (%s)" 
        (jdbc/as-sql-name (format "index_%s_%s" sql-table-name sql-column-name))
        sql-table-name
        sql-column-name))))

(defn create-indexes!
  [indexes]
  (doseq [index indexes]
    (create-index! index)))

(defn create-fk-constraint!
  [db [[left-table left-column] [right-table right-column]]]
  (let [sql-left-table (jdbc/as-sql-name (name left-table))
        sql-left-column (jdbc/as-sql-name (name left-column))
        sql-right-table (jdbc/as-sql-name (name right-table))
        sql-right-column (jdbc/as-sql-name (name right-column))]
    (jdbc/query db
      (format "alter table %s add constraint if not exists fk_%s_%s_%s_%s (%s) references %s (%s)"
        sql-left-table
        sql-left-table sql-left-column sql-right-table sql-right-column
        sql-left-column
        sql-right-table sql-right-column))))

(defn create-fk-constraints!
  [db fks] 
  (doseq [fk fks]
    (create-fk-constraint! db fk)))

(defn get-dump
  ([db k] (get-dump db k nil))
  ([db k entity]
    (let [result (jdbc/query (:conn db) 
                  ["select value from dump where key = ? and entity = ? limit 1"
                    k entity])
          result-string (first result)]
      (if result-string
        (fress/read (bs/convert result-string java.io.Reader))))))

(defn put-dump!
  ([db k v] (put-dump! db nil k v))
  ([db entity k v]
    (jdbc/insert! db :dump {
      :entity entity
      :key k
      :value (fress/write v)})))

(defn random-number
  [size]
  (->
    (SecureRandom/getInstance "SHA1PRNG")
    (.generateSeed size)))

(defn b64encode
  [ins]
  (let [encoder (java.util.Base64/getEncoder)]
    (.encodeToString encoder (bs/to-byte-array ins))))

(defn new-auth-token
  []
  (b64encode (random-number 256)))

(defn create-cache
  []
  (->
    (CacheBuilder/newBuilder)
    (.softValues)
    (.build)
    (.asMap)))

(defn setup-db!
  [db]
  (jdbc/with-db-transaction [txn (:conn db)]
    (create-tables! txn table-specs)
    (create-indexes! txn indexes)
    (create-fk-constraints! txn fk-constraints)
    (jdbc/execute! txn "create sequence thread_id_sequence")))

(defn init!
  [config]
  (let [fname (:subname (:db config))
        exists (.exists (io/file fname))
        conn (jdbc/get-connection (:db config))
        res {:conn conn
             :cache (create-cache)}]
    (if-not exists
      (setup-db! res))
    res))

(defn put!
  [^java.util.Map m k v]
  (.put m k v))

(defmacro cached
  [db cache-key value-generator]
  `(let [cache-key# ~cache-key
         cache-value# (get (:cache ~db) cache-key#)]
    (if cache-value#
      cache-value#
      (let [result# ~value-generator]
        (put! (:cache ~db) cache-key# cache-value#)
        cache-value#))))

(defn get-by-id
  [db table id]
  (cached db [:by-id table id]  
    (jdbc/query (:conn db) ["select * from ? where id = ? limit 1" (name table) id] {:result-set-fn first})))

(defn create-local-account!
  [db & {:keys [username password]}]
  (let [ph (hash-password password)
        auth-token (new-auth-token)]
    (jdbc/insert! (:conn db)
      {
        :username username
        :password_hash ph
        :auth_token auth-token})))

(defn new-thread-id!
  [db]
  (jdbc/query db "select next value for thread_id_sequence" {:result-set-fn first}))

(defn create-status!
  [db & {:keys [uri url account_id in_reply_to_id reblog content application_id]
         :else {:application_id nil :reblog nil :in_reply_to_id nil}}]
  (jdbc/with-db-transaction [tc (:conn db)]
    (let [reply-target (if in_reply_to_id (get-by-id tc :statuses in_reply_to_id))
          thread_id (if reply-target
                      (:thread_id reply-target)
                      (new-thread-id! tc))
          thread_depth (if reply-target (inc (:thread_depth reply-target)) 0)]
      (jdbc/insert! tc :statuses
        {
          :uri uri
          :url url
          :account_id account_id
          :in_reply_to_id in_reply_to_id
          :thread_id thread_id
          :thread_depth thread_depth
          :reblog reblog
          :content content
          :application_id application_id}))))

(defn get-thread
  [db status-id]
  (let [thread-id (:thread_id (get-by-id db :statuses status-id))]
    (jdbc/query db
      ["select id from statuses where thread_id = ? order by thread_depth" thread-id])))

(defn token-user
  [db auth-token]
  (cached db [:auth-token-account auth-token]
    (let [account-id (jdbc/query (:conn db) ["select id from accounts where auth_token = ? limit 1" auth-token] {:result-set-fn first})]
      ;; doing it this way so that both cache keys refer to the same object in memory
      (get-by-id db :accounts account-id))))


