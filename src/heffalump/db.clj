(ns heffalump.db
  (import [com.google.common.cache CacheBuilder]
          [javax.crypto SecretKey SecretKeyFactory]
          [javax.crypto.spec PBEKeySpec]
          [java.security SecureRandom])
  (require [clojure.java.jdbc :as jdbc]
           [clojure.data.fressian :as fress]
           [byte-streams :as bs]))

(defn create-table-if-not-exists-ddl
  "Given a table name and a vector of column specs, return the DDL string for
  creating that table. Each column spec is, in turn, a vector of keywords or
  strings that is converted to strings and concatenated with spaces to form
  a single column description in DDL, e.g.,
    [:cost :int \"not null\"]
    [:name \"varchar(32)\"]
  The first element of a column spec is treated as a SQL entity (so if you
  provide the :entities option, that will be used to transform it). The
  remaining elements are left as-is when converting them to strings.
  An options map may be provided that can contain:
  :table-spec -- a string that is appended to the DDL -- and/or
  :entities -- a function to specify how column names are transformed."
  ([table specs] (create-table-ddl table specs {}))
  ([table specs opts]
   (let [table-spec     (:table-spec opts)
         entities       (:entities   opts identity)
         table-spec-str (or (and table-spec (str " " table-spec)) "")
         spec-to-string (fn [spec]
                          (try
                            (str/join " " (cons (jdbc/as-sql-name entities (first spec))
                                                (map name (rest spec))))
                            (catch Exception _
                              (throw (IllegalArgumentException.
                                      "column spec is not a sequence of keywords / strings")))))]
     (format "CREATE TABLE IF NOT EXISTS %s (%s)%s"
             (jdbc/as-sql-name entities table)
             (str/join ", " (map jdbc/spec-to-string specs))
table-spec-str))))

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
        :reblog :int ; null or status
        :content :text ; Body of the status. Contains HTML
        :created_at :int ; UTC epoch timestamp of when this status was created
        ;; reblogs_count is a thing in the API response but that doesn't belong in the DB
        ;; likewise favourites_count
        :application_id :int ; which Application posted this
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
        :created_at :int]]
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
      ]]])
   
(def indexes
  [
    [:accounts :auth_token]
    [:mentions :status_id]
    [:mentions :recipient_id]
    [:media_attributes :status_id]])

(def fk-constraints
  [
    [[:media_attributes :status_id] [:statuses :id]]
    [[:mentions :status_id] [:statuses :id]]
    [[:statuses :account_id] [:accounts :id]]])

(defn create-tables!
  [tables]
  (doseq [[tablename columns] tables]
    (jdbc/query (create-table-if-not-exists-ddl tablename columns))))

(defn create-index!
  [[table-name column-name]]
  (let [sql-table-name (jdbc/as-sql-name (name table-name))
        sql-column-name (jdbc/as-sql-name (name column-name))]
    (jdbc/query
      (format "create index %s if not exists on %s (%s)" 
        (jdbc/as-sql-name (format "index_%s_%s" sql-table-name sql-column-nane))
        sql-table-name
        sql-column-name))))

(defn create-indexes!
  [indexes]
  (doseq [index indexes]
    (create-index! index)))

(defn create-fk-constraint!
  [[[left-table left-column] [right-table right-column]]]
  (let [sql-left-table (jdbc/as-sql-name (name left-table))
        sql-left-column (jdbc/as-sql-name (name left-column))
        sql-right-table (jdbc/as-sql-name (name right-table))
        sql-right-column (jdbc/as-sql-name (name right-column))]
    (jdbc/query
      (format "alter table %s add constraint fk_%s_%s_%s_%s (%s) references %s (%s)"
        sql-left-table
        sql-left-table sql-left-column sql-right-table sql-right-column
        sql-left-column
        sql-right-table sql-right-column))))

(defn create-fk-constraints
  [fks] 
  (doseq [fk fks]
    (create-fk-constraint! fk)))

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
    (jdbc/insert! db :dump {:entity entity :key k :value v})))

(defn random-number
  [size]
  (->
    (SecureRandom/getInstance "SHA1PRNG")
    (.generateSeed size)))

(def b64encode
  [ins]
  (let [encoder (java.util.Base64/getEncoder)]
    (.encodeToString encoder (bs/to-byte-array ins))))

(defn hash-password
  [^String password]
  (let [salt (random-number 32)
        key-factory (SecretKeyFactory/getInstance "PBKDF2WithHmacSHA1")
        crypt
          (.generateSecret key-factory
            (PBEKeySpec.
              (.toCharArray password)
              salt
              20000
              256))]
      (str (b64encode salt) "$" (b64encode crypt))))

(defn create-cache
  []
  (->
    (CacheBuilder/newBuilder)
    (.softValues)))

(defn setup-db
  [db]
  (jdbc/with-connection (:conn db)
    (jdbc/transaction
      (create-tables! tables)
      (create-indexes! indexes)
      (create-fk-contraints! fk-constraints))))

(defn init
  [config]
  (let [conn (:db config)]
    {:conn conn
     :cache (create-cache)}))
