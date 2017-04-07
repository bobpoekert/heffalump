(ns heffalump.db
  (import [com.google.common.cache CacheBuilder])
  (require [korma.core :as kc]))

(defn setup-db
  [db]
  (defentity statuses
    (entity-fields
      :id 	
      :uri     ;fediverse-unique resource ID
      :url     ;URL to the status page (can be remote)
      :account     ;Account
      :in_reply_to_id     ;null or ID of status it replies to
      :reblog     ;null or Status
      :content     ;Body of the status. This will contain HTML (remote HTML already sanitized)
      :created_at     ;
      :reblogs_count     ;
      :favourites_count     ;
      :reblogged     ;Boolean for authenticated user
      :favourited     ;Boolean for authenticated user
      :media_attachments     ;array of MediaAttachments
      :mentions     ;array of Mentions
      :application     ;Application from which the status was posted

(def-api-type media_attribute
  :url  ;URL of the original image (can be remote)
  :preview_url  ;URL of the preview image
  :type  ;Image or video
)

(def-api-type mention
  :url  ;URL of user's profile (can be remote)
  :acct  ;Username for local or username@domain for remote users
  :id  ;Account ID
)

(def-api-type account
  :id  ;
  :username  ;
  :acct  ;Equals username for local users, includes @domain for remote ones
  :display_name  ;
  :note  ;Biography of user
  :url  ;URL of the user's profile page (can be remote)
  :avatar  ;URL to the avatar image
  :header  ;URL to the header image
  :followers_count  ;
  :following_count  ;
  :statuses_count  ;
)

(defn setup-db
  [db]
  (doseq [[k v] api-type-fields]
    (defentity k
      (table (keyword (str (name k) s)))
      (entity-fields v)
      (database db))))

(defn create-cache
  []
  (->
    (CacheBuilder/newBuilder)
    (.softValues)))

(defn init
  [config]
  (let [conn (kc/create-db (:db config))]
    {:conn conn
     :cache (create-cache)}))
