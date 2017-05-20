(ns heffalump.db.account
  (:import IdList
          [gnu.trove.set.hash TLongHashSet])
  (:require [heffalump.db-utils :refer :all]
            [heffalump.db.status :refer [get-status]]
            [heffalump.db.core :refer :all]
            [manifold.deferred :as d]))

(def scopes [:public :private])
(def visibilities [
  :public :unlisted :private :direct])

(add-table! :accounts
    [id-column
    [:username :text]
    [:qualified_username :text]
    [:display_name :text]
    [:bio :text] ; biography of user
    [:url :text] ; profile page URL (can be remote)
    [:avatar :text] ; url of user's av image
    [:header :text] ; url of user's header image
    [:follower_count :int]
    [:following_count :int]
    [:posts_count :int]
    [:password_hash :text]
    [:auth_token :text]
    [:scope :int] ;; 0: public, 1: private
    [:keypair :clob]
    [:deleted_at :int]
    [:updated_at :int]
    [:friends_crawled :boolean]])

(add-table! :account_aliases
  [
    [:account_id :int]
    [:url :text]])
(add-table! :account_blocks
  [
    id-column
    [:blocker :int]
    [:blockee :int]])
(add-table! :account_mutes
  [
    id-column
    [:muter :int]
    [:mutee :int]])
(add-table! :account_follows
  [
    [:follower :int]
    [:followee :int]])

(add-table! :account_follow_requests
  [
    id-column
    [:follower :int]
    [:followee :int]
    [:created_at :int]])

(add-index! :accounts :auth_token)
(add-index! :accounts :username)
(add-index! :account_blocks :blocker)
(add-index! :account_mutes :muter)
(add-index! :account_follows :follower)
(add-index! :account_follows :followee)
(add-index! :account_aliases :url)

(defquery follows?
  [followee follower] [:follower]
  "select follower from account_follows where follower = ? and followee = ?"
  #(boolean (seq %)))

(defn add-follower!
  [db followee follower]
  (d/let-flow [follows (follows? db followee follower)]
    (if-not follows
      (insert-row! db :account_follows {:follower follower :followee followee}))))

(defn local-account?
  [account-row]
  (boolean (:password_hash account-row)))

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

(defn follow!
  [db followee-id follower-id]
  (d/let-flow [res (insert-row! db :account_follows {:follower follower-id :followee followee-id})]
    (if-let [^TLongHashSet cache (get-cache db [:followers followee-id])]
      (.add cache follower-id))
    res))

(defquery unfollow-query!
  [followee-id follower-id] []
  "delete from account_follows where followee = ? and follower = ?")

(defn unfollow!
  [db followee-id follower-id]
  (d/let-flow [res (unfollow-query! db followee-id follower-id)]
    (if-let [^TLongHashSet cache (get-cache db [:followers followee-id])]
      (.remove cache follower-id))
    res))

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
  (d/let-flow [ids (get-feed-ids db account-id)]
    (apply d/zip (map (partial get-status db) ids))))

(defn new-auth-token
  []
  (b64encode (random-number)))

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

(defn create-alias!
  [db account-id url]
  (insert-row! db :account_aliases {:account_id account-id :url url}))

(defn create-account-by-info!
  "creates an account row from an ostaus info map"
  [db info]
  (d/let-flow [res 
                (insert-row! db {
                  :avatar (:av info)
                  :qualified_username (:qualified-username info)
                  :display_name (:display-name info)
                  :username (:username info)
                  :bio (:bio info)})]
    (let [id (:id res)]
      (apply d/zip
        (for [a (if (:html-url info) (cons (:html-url info) (get info :ids [])) (get info :ids []))]
          (create-alias! db id a))))))

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

(defn deflate-account
  [account-row]
  (if account-row
    (assoc account-row :keypair (deserialize (:keypair account-row)))))

(defn get-account
  [db account-id]
  (d/chain (get-by-id db :accounts account-id) deflate-account))

(defquery qualified-username-query
  [username] [:id]
  "select id from accounts where qualified_username = ?"
  #(:id (first %))) 
  
(defn get-account-by-qualified-username
  [db username]
  (d/let-flow [acc-id (qualified-username-query db username)]
    (if acc-id
      (get-account db acc-id))))

(defquery account-alias-query
  [url] [:id]
  "select id from account_aliases where url = ?"
  #(:id (first %))) 

(defn get-account-by-alias
  [db url]
  (d/let-flow [id (account-alias-query db url)]
    (if id
      (get-account db id))))

(defn get-or-create-account-by-info!
  [db info]
  (d-cond?
    (:id info) (get-account db (:id info))
    (:qualified-username info) (get-account-by-qualified-username db (:qualified-username info))
    (:html-url info) (get-account-by-alias db (:html-url info))
    :else (create-account-by-info! db info)))

(defquery get-account-by-url-query
  [url] [:id]
  "select account_id from account_aliases where url = ?"
  #(mapv :id %))

(defn get-accounts-by-url
  [db url]
  (d/let-flow [ids (get-account-by-url-query url)]
    (apply d/zip
      (map (partial get-account) ids))))

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
  (d/let-flow [old-row (get-by-id db :accounts account-id)]
    (let [new-token (new-auth-token)
          old-token (:auth-token old-row)]
      (update-row! db :accounts {:id account-id :auth_token new-token})
      (delete-cache! db [:auth-token-id old-token]))))

(defn token-user
  [db auth-token]
  (d/let-flow [account-id (get-id-by-auth-token db auth-token)]
    ;; doing it this way so that both cache keys refer to the same object in memory
    (if account-id
      (get-by-id db :accounts account-id)
      d-nil)))
