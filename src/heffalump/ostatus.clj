(ns heffalump.ostatus
  (import [java.time Instant])
  (require [clojure.data.xml :refer-only [element alias-uri cdata]]
           [heffalump.db :as db]))

(defn iso-string
  [epoch-int]
  (.toString (Instant/ofEpochSecond epoch-int)))

;; reference: https://github.com/tootsuite/mastodon/blob/15ec4ae07b17821625bd2ca1088a7573a7ed128c/app/lib/atom_serializer.rb
;; https://github.com/tootsuite/mastodon/blob/a9529d3b4b057eeb3b47943b271ad6605e22732d/app/lib/tag_manager.rb

(alias-uri 'xmlns        "http://www.w3.org/2005/Atom")
(alias-uri 'media  "http://purl.org/syndication/atommedia")
(alias-uri 'activity     "http://activitystrea.ms/spec/1.0/")
(alias-uri 'thr    "http://purl.org/syndication/thread/1.0")
(alias-uri 'poco   "http://portablecontacts.net/spec/1.0")
(alias-uri 'dfrn   "http://purl.org/macgirvin/dfrn/1.0")
(alias-uri 'os     "http://ostatus.org/schema/1.0")
(alias-uri 'mtdn  "http://mastodon.social/schema/1.0")

(def verbs {
  :post           "http://activitystrea.ms/schema/1.0/post"
  :share          "http://activitystrea.ms/schema/1.0/share"
  :favorite       "http://activitystrea.ms/schema/1.0/favorite"
  :unfavorite     "http://activitystrea.ms/schema/1.0/unfavorite"
  :delete         "http://activitystrea.ms/schema/1.0/delete"
  :follow         "http://activitystrea.ms/schema/1.0/follow"
  :request_friend "http://activitystrea.ms/schema/1.0/request-friend"
  :authorize      "http://activitystrea.ms/schema/1.0/authorize"
  :reject         "http://activitystrea.ms/schema/1.0/reject"
  :unfollow       "http://ostatus.org/schema/1.0/unfollow"
  :block          "http://mastodon.social/schema/1.0/block"
  :unblock        "http://mastodon.social/schema/1.0/unblock"})

(def types {
  :activity   "http://activitystrea.ms/schema/1.0/activity"
  :note       "http://activitystrea.ms/schema/1.0/note"
  :comment    "http://activitystrea.ms/schema/1.0/comment"
  :person     "http://activitystrea.ms/schema/1.0/person"
  :collection "http://activitystrea.ms/schema/1.0/collection"
  :group "http://activitystrea.ms/schema/1.0/group"})

(def collections {
  :public "http://activityschema.org/collection/public"})

(def avatar-content-type "image/png")
(def header-content-type "image/png")

(defn author
  ([account] (author account :author))
  ([account tagname]
    (element tagname {}
      (element :id {} (:url account))
      (element ::activity/object-type {} (:person types))
      (element :uri {} (:url account))
      (element :name {} (:username account))
      (element :email {} (or (:text account) (format "@%s" (:username account))))
      (element :summary {} (:note account))
      (element :link {:rel "alternate" :type "text/html" :href (:url account)})
      (element :link {:rel "avatar" :type avatar-content-type :href (:avatar account)})
      (element :link {:rel "header" :type header-content-type :href (:header account)})
      (element ::poco/preferredUsername {} (:username account))
      (element ::poco/displayName {} (db/display-name account))
      (if (:note account)
        (element ::poco/note {} (:note account)))
      (element ::mtdn/scope {} (name (get db/scopes (:scope account)))))))

(defn serialize-status-attributes
  [status]
  (list
    (element :summary {} (db/status-slug status))
    (element :content {:type "text/html"} (cdata (db/status-html status)))
    (for [mentioned-uri (db/status-mention-uris status)]
      (element :link {
        :rel "mentioned"
        ::ostatus/object-type (:person types)
        :href (db/account-uri-to-url mentioned-uri)}))
    (element :link {
      :rel "mentioned"
      ::ostatus/object-type (:collection types)
      :href (:public collections)})
    (for [tag (db/status-tags status)]
      (element :category {:term tag}))
    (if (:nsfw status)
      (element :category {:term "nsfw"}))
    (for [media (:media status)]
      (element :link {
        :rel "enclosure"
        :type (:content_type media)
        :length (:file_size media)
        :href (:url media)}))
    (element ::mstdn/scope {}
      (name (nth db/visibilities (:visibility status))))))

(defn stream-entry
  ([action] (stream-entry action nil))
  ([action account]
    (element :entry {}
      (element :id (:id action))
      (element :published (iso-string (:created_at action)))
      (element :updated (iso-string (:created_at action)))
      (element :title (db/action-title action))
      (if account
        (author account))
      (element ::activity/object-type (get types (db/action-type action)))
      (element ::activity/verb (get verbs (get db/verbs (:verb action))))
      (if (:status action)
        (element ::activity/object {}
          (element :id {} (db/url-for-status (:status action)))
          (element :published {} (iso-string (:created_at (:status action))))
          (element :updated {} (iso-string (:created_at (:status action))))
          (element :title {} (db/status-title status))
          (author (:account (:status action)))
          (element ::activity/object-type (get types (db/action-type action)))
          (element ::activity/verb (get verbs (get db/verbs (:verb action))))
          (serialize-status-attributes (:status action))
          (element :link {
            :rel "alternate" :type "text/html" :href (db/status-url (:status action))})
          (element ::thr/in-reply-to {
            :ref (db/thread-uri (:status action)) 
            :href (db/thread-url (:status action))}))))))
        
(defn feed
  [account actions]
  (element :feed {}
    (element :id {} (:url account))
    (element :title {} (db/display-name account))
    (element :subtitle {} (:note account))
    (element :updated {} (iso-string (:updated_at account)))
    (element :logo {} (:avatar account))
    (author account)
    (element :link {:rel "alternate" :type "text/html" :href (db/url-for-account account :web)})
    (element :link {
      :rel "self" :type "application/atom+xml" :href (db/url-for-account account :atom)})
    (element :link {
      :rel "next" :type "application/atom+xml" :href (db/url-for-account account :atom {
        :max-id (max (map :id statuses))})})
    ielement :link {
      :rel "hub" :href api-push-url})
    (element :link {
      :rel "salmon" :href (db/url-for-account account :salmon)})
    (map stream-entry actions))
 
(defn object
  [status]
  (element ::activity/object {}
    (element :id {} (db/guid status))
    (element :published {} (iso-string (:created_at status)))
    (element :updated {} (iso-string (:updated_at status)))
    (element :title {} (db/status-title status))
    (author (:account status))
    (element ::activity/object-type {} (get types (db/status-object-type status)))
    (element ::activity/verb {} (get verbs (db/status-verb status)))
    (serialize-status-attributes status)
    (element :link {:rel "alternate" :type "text/html" :href (db/status-url status)})
    (element ::thr/in-reply-to {
      :ref (db/thread-uri status)
      :href (db/thread-url status)})))

(defprotocol ToSalmon
  (to-salmon [v]))

(extend-protocol ToSalmon
  db/FollowAction
  (to-salmon [action]
    (let [description (format "%s started following %s"
                        (:username (:follower action)) 
                        (:username (:followee action)))]
      (element :entry {}
        (element :id {} (db/guid action))
        (element :title {} description)
        (element :content {:type "html"} description)
        (author (:follower action))
        (element ::activity/object-type {} (:activity types))
        (element ::activity/verb {} (:unfollow verbs))
        (author (:followee action) ::activity/object))))
  db/FollowRequestAction
  (to-salmon [action]
    (element :entry {}
      (element :id {} (db/guid action))
      (element :title {} (format "%s requested to follow %s"
                          (:username (:follower action))
                          (:username (:followee action))))
      (author (:follower action))
      (entry ::activity/object-type {} (:activity types))
      (entry ::activity/verb {} (:request_friend verbs))
      (author (:followee action) ::activity/object)))
  db/AuthorizeAction
  (to-salmon [action]
    (element :entry {}
      (element :id {} (db/guid action))
      (element :title {} (format "%s authorizes follow request by %s"
                          (:username (:followee action))
                          (:username (:follower action))))
      (author (:followee action))
      (element ::activity/object-type {} (:activity types))
      (element ::activity/verb {} (:request_friend verbs))
      (element ::activity/object {} 
        (author (:followee action))
        (element ::activity/object-type {} (:activity types))
        (element ::activity/verb {} (:authorize types)))
      (element ::activity/object {}
        (author (:follower action))
        (element ::activity/object-type {} (:activity types))
        (element ::activity/verb {} (:request_friend verbs))
        (author (:followee action) ::activity/object))))
  db/RejectAction
  (to-salmon [action]
    (element :entry {}
      (element :id {} (db/guid action)) 
      (element :title {} (format "%s rejects follow request by %s"
                          (:username (:followee action))
                          (:username (:follower action))))
      (author (:followee action))
      (element ::activity/object-type {} (:activity types))
      (element ::activity/verb {} (:request_friend verbs))
      (author (:follower action) ::activity/object)))
  db/UnfollowAction
  (to-salmon [action]
    (let [description (format "%s is no longer following %s"
                        (:username (:follower action))
                        (:username (:followee action)))]
      (element :entry {}
        (element :id {} (db/guid action))
        (element :title {} description)
        (element :content {:type "text/html"} description)
        (author (:follower action))
        (element ::activity/object-type {} (:activity types))
        (element ::activity/verb {} (:unfollow types))
        (author (:followee action) ::activity/object))))
  db/BlockAction
  (to-salmon [action]
    (let [description (format "%s no longer wishes to interact with %s"
                        (:username (:blocker action))
                        (:username (:blockee action)))]
      (element :entry {}
        (element :id {} (db/guid action))
        (element :title {} description)
        (author (:blocker action))
        (element ::activity/object-type (:activity types))
        (element ::activity/verb (:block verbs))
        (author (:blockee action) ::activity/object))))
  db/UnblockAction
  (to-salmon [action]
    (element :entry {}
      (element :id {} (db/guid action))
      (element :title {} (format "%s no longer blocks %s" (:username (:blocker action)) (:username (:blockee action))))
      (author (:blocker action))
      (element ::activity/object-type {} (:activity types))
      (element ::activity/verb {} (:unblock verbs))
      (author (:blockee action) ::activity/object)))
  db/ShareAction
  (to-salmon [action]
    (stream-entry action))
  db/FavAction
  (to-salmon [action]
    (let [description (format "%s fav'd a status by %s" (:username (:faver action)) (:username (:account (:status action))))]
      (element :entry {}
        (element :id {} (db/guid action))
        (element :title {} description)
        (element :content {:type "html"} description)
        (author (:faver action))
        (element ::activity/object-type {} (:activity types))
        (element ::activity/verb {} (:favorite verbs))
        (object (:status action))
        (element ::thr/in-reply-to {
          :ref (db/thread-uri (:status action))
          :href (db/thread-url (:status action))}))))
  db/UnfavAction
  (to-salmon [action]
    (let [description (format "%s unfav'd a status by %s" (:username (:faver action)) (:username (:account (:status action))))]
      (element :entry {}
        (element :id {} (db/guid action))
        (element :title {} description)
        (element :content {:type "html"} description)
        (author (:faver action))
        (element ::activity/object-type {} (:activity types))
        (element ::activity/verb {} (:unfavorite verbs))
        (object (:status action))
        (element ::thr/in-reply-to {
          :ref (db/thread-uri (:status action))
          :href (db/thread-url (:status action))})))))
