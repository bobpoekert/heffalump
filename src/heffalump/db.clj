(ns heffalump.db
  (import IdList)
  (require [heffalump.db-utils :refer :all]
           [clojure.string :as s]
           [clojure.java.io :as io]
           [clojure.java.jdbc :as jdbc]
           [clojure.data.fressian :as fress]
           [byte-streams :as bs]))

(def id-column [:id "INTEGER NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 1, increment by 1)"])

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
        [:mutee :int]]]
    [:account_follows
      [
        [:follower :int]
        [:followee :int]]]])
   
(def indexes
  [
    [:accounts :auth_token]
    [:accounts :username]
    [:mentions :status_id]
    [:mentions :recipient_id]
    [:media_attributes :status_id]
    [:statuses :thread_id]
    [:account_blocks :blocker]
    [:account_mutes :muter]
    [:account_follows :follower]
    [:account_follows :followee]])

(defquery get-followers
  [account-id] [:id]
  "select followee from account_follows where follower = ?"
  #(mapv :id %))

(defquery get-following
  [account-id] [:id]
  "select follower from account_follows where followee = ?"
  #(mapv :id %))

(defquery get-feed-query
  [account-id] [:id]
  "select statuses.id from statuses where statuses.account_id in (
    select followee from account_follows where follower = ?
  ) order by statuses.id"
  #(IdList. 1000 (take 1000 (map :id %))))

(defn get-feed-ids
  [db account-id]
  (cached db [:account-feed account-id]
    (get-feed-query db account-id 1000)))

(defn get-feed-statuses
  [db account-id]
  (mapv #(get-by-id db :statuses %) (get-feed-ids db account-id)))

(defn update-feed!
  [db post-id follower-id]
  (if-let [^IdList cached-feed (get-cache db [:account-feed follower-id])]
    (.add cached-feed post-id)))

(defn new-post-update-feeds!
  [db poster-id post-id]
  (let [followers (get-followers db poster-id)]
    (doseq [follower followers]
      (update-feed! db post-id follower))))

(defquery get-dump-query
  [k entity] [:v]
  "select v from dump where k = ? and entity = ?")

(defn get-dump
  ([db k] (get-dump db k nil))
  ([db k entity]
    (let [result-string (:v (get-dump-query db k entity))]
      (if result-string
        (fress/read (bs/convert result-string java.io.Reader))))))

(defn put-dump!
  ([db k v] (put-dump! db nil k v))
  ([db entity k v]
    (insert-row! db :dump {
      :entity entity
      :k k
      :v (fress/write v)})))

(defn new-auth-token
  []
  (b64encode (random-number 32)))

(defn setup-db!
  [db]
  (jdbc/with-db-transaction [txn db]
    (create-tables! txn table-specs)
    (create-indexes! txn indexes)
    (jdbc/execute! txn "create sequence thread_id_sequence start with 0")
    (jdbc/execute! txn "create sequence clock_sequence start with 0")))

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
          (setup-db! db)
          (swap! db-setup? (fn [v] true)))
        (assoc
          (populate-queries db table-specs)
          :cache cache)))))

(defn create-local-account!
  [db acc]
  (let [ph (hash-password (:password acc))
        auth-token (new-auth-token)]
    (insert-row! db :accounts
      {
        :username (:username acc)
        :password_hash ph
        :auth_token auth-token})))

(defquery new-thread-id!
  [] [:v]
  "VALUES NEXT VALUE FOR thread_id_sequence"
  #(:v (first %)))

(defquery new-clock!
  [] [:v]
  "VALUES NEXT VALUE FOR clock_sequence"
  #(:v (first %)))

(defquery log-in-query
  [username] [:password_hash :auth_token]
  "select password_hash, auth_token from accounts where username = ?"
  first)

(defn log-in
  [db username password]
  (let [res (log-in-query db username)]
    (if (check-password (:password_hash res) password)
      (:auth_token res)
      nil)))

(defn create-status!
  [db & {:keys [uri url account_id in_reply_to_id reblog content application_id]
         :else {:application_id nil :reblog nil :in_reply_to_id nil}}]
  (jdbc/with-db-transaction [tc db]
    (let [reply-target (if in_reply_to_id (get-by-id tc :statuses in_reply_to_id))
          thread_id (if reply-target
                      (:thread_id reply-target)
                      (new-thread-id! tc))
          thread_depth (if reply-target (inc (:thread_depth reply-target)) 0)
          status-id
            (insert-row! tc :statuses
              {
                :uri uri
                :url url
                :account_id account_id
                :in_reply_to_id in_reply_to_id
                :thread_id thread_id
                :thread_depth thread_depth
                :reblog reblog
                :content content
                :application_id application_id})]
      (new-post-update-feeds! db account_id status-id))))

(defquery get-ids-by-thread-id
  [thread-id] [:id]
  "select id from statuses where thread_id = ? order by thread_depth")

(defn get-thread
  [db status-id]
  (mapv
    (fn [id] (get-by-id db :statuses id))
    (get-ids-by-thread-id db
      #(mapv :id %)
      (:thread_id (get-by-id db :statuses status-id)))))
    
(defquery get-id-by-auth-token-query
  [auth-token] [:id]
  "select id from accounts where auth_token = ?"
  #(:id (first %)))

(defn get-id-by-auth-token
  [db auth-token]
  (cached db [:auth-token-id auth-token]
    (get-id-by-auth-token-query db auth-token))) 

(defn change-auth-token!
  [db account-id]
  (let [new-token (new-auth-token)
        old-token (:auth-token (get-by-id db :accounts account-id))]
    (update-row! db :accounts {:id account-id :auth_token new-token})
    (delete-cache! db [:auth-token-id old-token])))

(defn token-user
  [db auth-token]
  (if-let [account-id (get-id-by-auth-token db auth-token)]
    ;; doing it this way so that both cache keys refer to the same object in memory
    (get-by-id db :accounts account-id)))


