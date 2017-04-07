(ns heffalump.app
  (require
    [heffalump.db :as hdb]
    [aleph.http :as http]
    [compojure.core :as cj]
    [byte-streams :as bs]
    [manifold.stream :as s]
    [manifold.deferred :as d]
    [manifold.bus :as bus]))

(defn create-app-state
  [db config]
  {
    :db db
    :config config})

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
    {:handler (params/wrap-params (routes))}))

(defn run!
  [db config]
  (http/start-server (create-app db config) {:port (:port config)}))
