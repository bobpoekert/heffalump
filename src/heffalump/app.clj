(ns heffalump.app
  (:require
    [heffalump.db :as db]
    [clojure.string :as s] 
    [aleph.http :as http]
    [ring.middleware.cookies :refer-only [wrap-cookies]]
    [ring.middleware.params :refer-only [wrap-params]]
    [compojure.core :as cj]
    [byte-streams :as bs]
    [manifold.stream :as ms]
    [manifold.deferred :as d]
    [manifold.bus :as bus]
    [manifold.executor :as ex]
    [cheshire.core :as json]))

(defn create-app-state
  [db config]
  {
    :db db
    :config config})

(defn in-db
  [app-state thunk & fargs]
  (->
    (d/future (apply thunk (:db app-state) fargs))
    (d/onto (:db-executor app-state))))

(def auth-fail {
  :status 401
  :body (json/generate-string {
    :error "The access token is invalid"})})

(defn auth-token
  [req]
  (or
    (:auth_token (:cookies req))
    (if-let [auth-header (get (:headers req) "Authorization")]
      (if (s/starts-with? auth-header "Bearer ")
        (.substring ^String auth-header #=(.length "Bearer "))))))

(defn authed
  [app-state thunk]
  (fn [req]
    (let [token (auth-token req)]
      (if-not token
        auth-fail
        (d/let-flow [auth-user (db/token-user (:db app-state) token)]
          (if auth-user
            (thunk req auth-user)
            auth-fail))))))

(defn creates-status
  [app-state]
  (->
    (authed app-state)
    (fn [req auth-user]
      (let [account-id (:id auth-user)
            reply-id (:in_reply_to_id req)
            content (:content req)]
        (d/let-flow [db-res (db/create-status! (:db app-state) {
                              :account_id account-id
                              :in_reply_to_id reply-id
                              :content content})]
          {:id (:id db-res)})))))

(defn create-routes
  [app-state]
  (cj/routes
    (cj/context "/api/v1"
      (cj/POST "/statuses" (creates-status app-state))
      (cj/POST "/media" (creates-media app-state))
      (cj/GET "/timelines/home" (gets-home-timeline app-state))
      (cj/GET "/timelines/mentions" (gets-mentions-timeline app-state))
      (cj/GET "/timelines/public" (gets-public-timeline app-state))
      (cj/GET "/timelines/tag/:hashtag" (gets-tag-timeline app-state))
      (cj/GET "/notifications" (gets-notifications app-state))
      (cj/POST "/follows" (creates-follow app-state))
      (cj/GET "/statuses/:id" (gets-status app-state))
      (cj/GET "/accounts/:id" (gets-account app-state))
      (cj/GET "/accounts/verify_credentials" (gets-authed-user app-state))
      (cj/GET "/accounts/:id/statuses" (gets-statuses app-state))
      (cj/GET "/accounts/:id/following" (gets-following app-state))
      (cj/GET "/accounts/:id/followers" (gets-followers app-state))
      (cj/GET "/accounts/relationships" (gets-relationships app-state))
      (cj/GET "/accounts/search" (gets-account-search app-state))
      (cj/GET "/blocks" (gets-blocks app-state))
      (cj/GET "/favourites" (gets-favs app-state))
      (cj/DELETE "/statuses/:id" (deletes-status app-state))
      (cj/POST "/statuses/:id/reblog" (creates-status-reblog app-state))
      (cj/POST "/statuses/:id/unreblog" (deletes-status-reblog app-state))
      (cj/POST "/statuses/:id/favourite" (creates-fav app-state))
      (cj/POST "/statuses/:id/unfavourite" (deletes-fav app-state))
      (cj/GET "/statuses/:id/context" (gets-thread app-state))
      (cj/GET "/statuses/:id/reblogged_by" (gets-reblogged-by app-state))
      (cj/GET "/statuses/:id/favourited_by" (gets-faved-by app-state))
      (cj/POST "/accounts/:id/follow" (creates-follow app-state))
      (cj/POST "/accounts/:id/unfollow" (deletes-follow app-state))
      (cj/POST "/accounts/:id/block" (creates-block app-state))
      (cj/POST "/accounts/:id/unblock" (deletes-block app-state)))))

(defn create-app
  [db config]
  (let [state (create-app-state db config)
        routes (create-routes config state)]
    {:handler (->
                (wrap-params)
                (wrap-cookies)
                (routes))}))

(defn run!
  [db config]
  (http/start-server
    (create-app db config)
    {:port (:port config)
     :executor :none
     :compression? true}))
