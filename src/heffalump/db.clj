(ns heffalump.db
  (import [com.google.common.cache CacheBuilder]
          [javax.crypto SecretKey SecretKeyFactory]
          [javax.crypto.spec PBEKeySpec]
          [java.security SecureRandom]
          [org.mindrot.jbcrypt BCrypt]
          ThreadLocalThing)
  (require [heffalump.db-utils :refer :all]
           [clojure.string :as s]
           [clojure.java.io :as io]
           [clojure.java.jdbc :as jdbc]
           [clojure.data.fressian :as fress]
           [byte-streams :as bs]))


(def id-column [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"])

(def table-specs
  [
    [:dump
      [
        [:entity :int]
        [:k :text]
        [:v :text]]]
    [:statuses
      ;; statuses are immutable with the exception of the deleted_at column
      [
        id-column
        [:uri :text] ;fediverse-unique resource ID
        [:url :text] ;URL to the status page (can be remote)
        [:account_id :int] ; Account FK
        [:in_reply_to_id :int] ; null or ID of status it replies to
        ;; on status insert, if the status is in_reply_to another status,
        ;; this status has the same thread id. otherwise, thread_id = id
        [:thread_id :int]
        [:thread_depth :int]
        [:reblog :int] ; null or status
        [:content :text] ; Body of the status. Contains HTML
        [:created_at :int] ; UTC epoch timestamp of when this status was created
        ;; reblogs_count is a thing in the API response but that doesn't belong in the DB
        ;; likewise favourites_count
        [:application_id :int] ; which Application posted this
        [:deleted_at :int]
        ]]
    [:media_attributes
      [
        id-column
        [:status_id :int]
        [:url :text] ; image asset URL
        [:preview_url :text] ; image preview (ie: resized) asset url
        [:type :int] ; 0 for image and 1 for video
      ]]
    [:mentions
      [
        id-column
        [:status_id :int]
        [:sender_id :int]
        [:recipient_id :int]
        [:created_at :int]
        [:deleted_at :int]]]
    [:accounts
      [
      id-column
      [:username :text]
      [:acct :text] ; null for local users (use username), username@domain for remote
      [:display_name :text]
      [:note :text] ; biography of user
      [:url :text] ; profile page URL (can be remote)
      [:avatar :text] ; url of user's av image
      [:header :text] ; url of user's header image
      ;; followers_count, following_count, and statuses_count can be computed
      [:password_hash :text]
      [:auth_token :text]
      [:deleted_at :int]
      ]]
    [:account_blocks
      [
        id-column
        [:blocker :int]
        [:blockee :int]]]
    [:account_mutes
      [
        id-column
        [:muter :int]
        [:mutee :int]]]])
   
(def indexes
  [
    [:accounts :auth_token]
    [:mentions :status_id]
    [:mentions :recipient_id]
    [:media_attributes :status_id]
    [:statuses :thread_id]
    [:account_blocks :blocker]
    [:account_mutes :muter]])

(def fk-constraints
  [
    [[:media_attributes :status_id] [:statuses :id]]
    [[:mentions :status_id] [:statuses :id]]
    [[:statuses :account_id] [:accounts :id]]])


(defn get-dump
  ([db k] (get-dump db k nil))
  ([db k entity]
    (let [result (jdbc/query db 
                  ["select v from dump where k = ? and entity = ? limit 1"
                    k entity])
          result-string (first result)]
      (if result-string
        (fress/read (bs/convert result-string java.io.Reader))))))

(defn put-dump!
  ([db k v] (put-dump! db nil k v))
  ([db entity k v]
    (jdbc/insert! db :dump {
      :entity entity
      :k k
      :v (fress/write v)})))

(defn new-auth-token
  []
  (b64encode (random-number 256)))

(defn setup-db!
  [db]
  (println "setting up db")
  (jdbc/with-db-transaction [txn db]
    (println "create tables")
    (create-tables! txn table-specs)
    (println "create indexes")
    (create-indexes! txn indexes)
    ;(println "craete constraints")
    ;(create-fk-constraints! txn fk-constraints)
    (println "create sequence")
    (jdbc/execute! txn "create sequence thread_id_sequence")
    (jdbc/execute! txn "create sequence clock_sequence")))

(defn init!
  [config]
  (let [db-setup? (atom false)] 
    (thread-local
      (let [db (create-db config)
            pathname (:subname (:db config))]
        (println pathname)
        (when (and (not @db-setup?)
                   (:init-db config)
                   (or (.startsWith ^String pathname "memory:")
                       (not (.exists (io/file pathname)))))
          (setup-db! db)
          (swap! db-setup? (fn [v] true)))
        (populate-queries db table-specs)))))


(defn create-local-account!
  [db & {:keys [username password]}]
  (let [db @db
        ph (hash-password password)
        auth-token (new-auth-token)]
    (jdbc/insert! db :accounts
      {
        :username username
        :password_hash ph
        :auth_token auth-token})))

(defn new-thread-id!
  [db]
  (query-one db "select next value for thread_id_sequence"))

(defn create-status!
  [db & {:keys [uri url account_id in_reply_to_id reblog content application_id]
         :else {:application_id nil :reblog nil :in_reply_to_id nil}}]
  (let [db @db]
    (jdbc/with-db-transaction [tc db]
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
            :application_id application_id})))))

(defn get-thread
  [db status-id]
  (let [db @db
        thread-id (:thread_id (get-by-id db :statuses status-id))]
    (jdbc/query db
      ["select id from statuses where thread_id = ? order by thread_depth" thread-id])))

(defn token-user
  [db auth-token]
  (let [db @db]
    (cached db [:auth-token-account auth-token]
      (let [account-id (query-one db ["select id from accounts where auth_token = ? limit 1" auth-token])]
        ;; doing it this way so that both cache keys refer to the same object in memory
        (get-by-id db :accounts account-id)))))


