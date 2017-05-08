(ns heffalump.ostatus.util
  (:import [java.time Instant]))

(defn iso-string
  [epoch-int]
  (.toString (Instant/ofEpochSecond epoch-int)))

(def namespaces {
  :xmlns        "http://www.w3.org/2005/Atom"
  :media  "http://purl.org/syndication/atommedia"
  :activity     "http://activitystrea.ms/spec/1.0/"
  :thr    "http://purl.org/syndication/thread/1.0"
  :poco   "http://portablecontacts.net/spec/1.0"
  :dfrn   "http://purl.org/macgirvin/dfrn/1.0"
  :os     "http://ostatus.org/schema/1.0"
  :mtdn  "http://mastodon.social/schema/1.0"
  :magic "http://salmon-protocol.org/ns/magic-env"})


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

