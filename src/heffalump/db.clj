(ns heffalump.db
  (import IdList
          [gnu.trove.set.hash TLongHashSet])
  (require [heffalump.db-utils :refer :all]
           [clojure.string :as s]
           [clojure.java.io :as io]
           [clojure.java.jdbc :as jdbc]
           [clojure.data.fressian :as fress]
           [byte-streams :as bs]))

(def id-column [:id "INTEGER NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 1, increment by 1)"])

(def verbs [
  :follow ; [follower:Account, followee:Account]
  :request_friend ; [follower:Account, followee:Account]
  :authorize ; [follow-request-id]
  :reject ; [follow-request-id]
  :unfollow ; [follower:Account, followee:Account]
  :block ; [blocker:Account, blockee:Account]
  :unblock ; [blocker:Account, blockee:Account]
  :share ; [post:Status] (:reblog for this status should be non-null)
  :favorite ; [faver:Account, post:Status]
  :unfavorite ; [faver:Account, post:Status]
])

(defrecord FollowAction [follower followee])
(defrecord FriendRequestAction [follower followee])
(defrecord AuthorizeAction [follow-request follower followee])
(defrecord RejectAction [follow-request follower followee])
(defrecord UnfollowAction [follower followee])
(defrecord BlockAction [blocker blockee])
(defrecord UnblockAction [blocker blockee])
(defrecord ShareAction [status])
(defrecord FavAction [faver status])
(defrecord UnfavAction [faver status])

(defn inflate-log-action
  [db row]
  (case (nth verbs (:verb db))
    :follow (FollowAction.
              (get-by-id db :accounts (:source row))
              (get-by-id db :accounts (:target row)))
    :request_friend (FriendRequestAction.
                      (get-by-id :accounts (:source row))
                      (get-by-id :accounts (:target row)))
    :authorize (let [follow-request (get-by-id db :follow_requests (:target row))]
                (AuthorizeAction.
                  follow-request
                  (get-by-id db :accounts (:follower follow-request))
                  (get-by-id db :accounts (:followee follow-request))))
    :reject (let [follow-request (get-by-id db :follow_requests (:target row))]
              (RejectAction.
                follow-request
                (get-by-id db :accounts (:source follow-request))
                (get-by-id db :accounts (:target follow-request))))
    :unfollow (UnfollowAction.
                (get-by-id db :accounts (:source row))
                (get-by-id db :accounts (:target row)))
    :share (ShareAction.
            (get-status db (:target row)))
    :favorite (FavAction.
                (get-by-id db :accounts (:source row))
                (get-status db (:target row)))))

(defprotocol ActionType
  (action-type [v]))

(extend-protocol ActionType
  FollowAction
  (action-type [v] :follow)
  FriendRequestAction
  (action-type [v] :request_friend)
  AuthorizeAction
  (action-type[v] :authorize)
  RejectAction
  (action-type [v] :reject)
  UnfollowAction
  (action-type [v] :unfollow)
  BlockAction
  (action-type [v] :block)
  UnblockAction
  (action-type [v] :unblock)
  ShareAction
  (action-type [v] :share)
  FavAction
  (action-type [v] :favorite))

(def scopes [:public :private])
(def visibilities [
  :public :unlisted :private :direct])

(def table-specs
  [
    [:dump
      [
        [:entity :int]
        [:k :text]
        [:v :text]]]
    [:action_log
      [
        id-column
        [:created_at :int]
        [:source :int]
        [:target :int]
        [:verb :int]]]
    [:follow_requests
      [
        id-column
        [:follower :int]
        [:followee :int]
        [:created_at :int]]]
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
        [:content :text] ; Body of the status
        [:created_at :int] ; UTC epoch timestamp of when this status was created
        ;; reblogs_count is a thing in the API response but that doesn't belong in the DB
        ;; likewise favourites_count
        [:nsfw :boolean]
        [:cw_text :text]
        [:verb :int]
        [:application_id :int] ; which Application posted this
        [:deleted_at :int]
        [:visibility :int]
        ]]
    [:hubub_subs
      [
        id-column
        [:account_id :int]
        [:callback_url :text]
        [:confirmed :boolean]
        [:expires_at :int]
        [:confirmation_secret "VARCHAR(200)"]]]
    [:media_attributes
      [
        id-column
        [:status_id :int]
        [:url :text] ; image asset URL
        [:preview_url :text] ; image preview (ie: resized) asset url
        [:content_type "VARCHAR(64)"] ; mime type
        [:file_size :int]
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
      [:scope :int] ;; 0: public, 1: private
      [:keypair :clob]
      [:deleted_at :int]
      [:updated_at :int]
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
    [:stream_entries :account_id]
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

(defn display-name
  [account]
  (or
    (:display_name account)
    (:username account)))

(defn url-for-account
  [account]
  "")

(defquery get-followers-query
  [account-id] [:id]
  "select followee from account_follows where follower = ?"
  #(mapv :id %))

(defquery get-following
  [account-id] [:id]
  "select follower from account_follows where followee = ?"
  #(mapv :id %))

(defn get-followers
  [db account-id]
  (cached db [:followers account-id]
    (get-followers-query db 
      #(TLongHashSet. (map :id %))
      account-id)))

(defquery get-media-attrs
  [status-id] [:url :preview_url :content_type :file_size]
  "select url, preview_url, content_type, file_size from media_attributes where status_id = ?")

(defn follow!
  [db followee-id follower-id]
  (insert-row! db :account_follows {:follower follower-id :followee followee-id})
  (if-let [^TLongHashSet cache (get-cache db [:followers followee-id])]
    (.add cache follower-id)))

(defquery unfollow-query!
  [followee-id follower-id] []
  "delete from account_follows where followee = ? and follower = ?")

(defn unfollow!
  [db followee-id follower-id]
  (unfollow-query! db followee-id follower-id)
  (if-let [^TLongHashSet cache (get-cache db [:followers followee-id])]
    (.remove cache follower-id)))

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
  (mapv (partial get-status db) (get-feed-ids db account-id)))

(defn update-feed!
  [db post-id follower-id]
  (if-let [^IdList cached-feed (get-cache db [:account-feed follower-id])]
    (.put cached-feed post-id)))

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

(defquery blocked-query
  [blocker blockee] []
  "select from account_blocks where blocker = ? and blockee = ?"
  #(boolean (seq %)))

(defn blocks?
  [db blocker blockee]
  (cached db [:blocks? (id-or-int blocker) (id-or-int blockee)]
    (blocked-query db (id-or-int blocker) (id-or-int blockee))))

(defn block!
  [db blocker blockee]
  (if-not (blocks? db blocker blockee)
    (let [blocker (id-or-int blocker) blockee (id-or-int blockee)]
      (inert-row! db :account_blocks {:blocker blocker :blockee blockee})
      (put-cache! db [:blocks? blocker blockee] true))))

(defquery unblock-query
  [blocker blockee] []
  "delete from account_blocks where blocker = ? and blockee = ?")

(defn unblock!
  [db blocker blockee]
  (let [blocker (id-or-int blocker) blockee (id-or-int blockee)]
    (unblock-query db blocker blockee)
    (put-cache! db [:blocks? blocker blockee] false)))

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
        auth-token (new-auth-token)
        keypair (rsa-keypair)]
    (insert-row! db :accounts
      {
        :username (:username acc)
        :password_hash ph
        :auth_token auth-token
        :rsa_keypair (serialize keypair)})))

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
          status
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
      (new-post-update-feeds! db account_id (:id status)))))

(defquery delete-status-query
  [status-id] []
  "update statuses set deleted_at = now() where id = ?")

(defn delete-status!
  [db status-id]
  (delete-status-query db status-id)
  (let [cache-key [:by-id-results :statuses status-id]]
    (if (get-cache db cache-key)
      (put-cache! db cache-key :deleted))))

(defn get-account
  [db account-id]
  (let [res (get-by-id db :accounts account-id)]
    (assoc res :keypair (deserialize (:keypair res)))))

(defn get-status
  [db status-id]
  (let [res (get-by-id db :statuses status-id)]
    (if (or (= res :deleted) (:deleted_at res))
      nil
      (->
        res
        (assoc :account (get-account db (:account_id res)))
        (assoc :media (get-media-attrs db status-id))))))

(defquery get-ids-by-thread-id
  [thread-id] [:id]
  "select id from statuses where thread_id = ? order by thread_depth")

(defn status-html
  [status]
  (format "<pre>%s</pre>" (:body status)))

(defn status-tags
  [status]
  '())

(defn get-thread
  [db status-id]
  (vec
    (filter #(not (= % :deleted))
      (map
        (partial get-status db)
        (get-ids-by-thread-id db
          #(mapv :id %)
          (:thread_id (partial get-status db)))))))
    
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
