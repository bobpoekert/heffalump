(ns heffalump.db.status
  (:require [heffalump.db-utils :refer :all]
            [heffalump.db.core :refer :all]
            [heffalump.db.account :refer [get-followers get-account]]
            [manifold.deferred :as d]))

(add-table! :statuses
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
        [:content :clob] ; Body of the status
        [:created_at :int] ; UTC epoch timestamp of when this status was created
        ;; reblogs_count is a thing in the API response but that doesn't belong in the DB
        ;; likewise favourites_count
        [:nsfw :boolean]
        [:cw_text :text]
        [:verb :int]
        [:application_id :int] ; which Application posted this
        [:deleted_at :int]
        [:visibility :int]
        ])

(add-table! :status_tags
      [
        [:status_id :int]
        [:tag :text]])
(add-table! :hubub_subs
      [
        id-column
        [:account_id :int]
        [:callback_url :text]
        [:confirmed :boolean]
        [:expires_at :int]
        [:confirmation_secret "VARCHAR(200)"]])
(add-table! :media_attributes
      [
        id-column
        [:status_id :int]
        [:url :text] ; image asset URL
        [:preview_url :text] ; image preview (ie: resized) asset url
        [:content_type "VARCHAR(64)"] ; mime type
        [:file_size :int]
      ])

(add-index! :media_attributes :status_id)
(add-index! :statuses :thread_id)
(add-index! :status_tags :status_id)

(defquery get-media-attrs
  [status-id] [:url :preview_url :content_type :file_size]
  "select url, preview_url, content_type, file_size from media_attributes where status_id = ?")

(defquery get-tags
  [status-id] [:tag]
  "select tag from status_tags where status_id = ?"
  #(mapv :tag %))

(defquery new-thread-id!
  [] [:v]
  "VALUES NEXT VALUE FOR thread_id_sequence"
  #(:v (first %)))

(defn update-feed!
  [db post-id follower-id]
  (d/let-flow [^IdList cached-feed (get-cache db [:account-feed follower-id])]
    (if cached-feed
      (.put cached-feed post-id))))

(defn new-post-update-feeds!
  [db poster-id post-id]
  (d/let-flow [followers (get-followers db poster-id)]
    (doseq [follower followers]
      (update-feed! db post-id follower))))

(defn create-status!
  [db status]
  (let [in_reply_to_id (:in_reply_to_id status)
        account_id (:account_id status)]
    (d/let-flow [reply-target (d-if in_reply_to_id (get-by-id db :statuses in_reply_to_id))
                 thread_id (d-if (not reply-target) (new-thread-id! db))]
      (let [thread_id (or thread_id (:thread_id reply-target))
            thread_depth (if reply-target (inc (:thread_depth reply-target)) 0)]
        (d/let-flow [status (insert-row! db :statuses status)]
          (new-post-update-feeds! db account_id (:id status)))))))

(defquery delete-status-query
  [delete-time status-id] []
  "update statuses set deleted_at = ? where id = ?")

(defn delete-status!
  [db status-id]
  (delete-status-query db (now) status-id)
  (let [cache-key [:by-id-results :statuses status-id]]
    (if (get-cache db cache-key)
      (put-cache! db cache-key :deleted))))

(defn get-status
  [db status-id]
  (d/let-flow [res (get-by-id db :statuses status-id)]
    (if (or (= res :deleted) (:deleted_at res))
      nil
      (d/let-flow [account (get-account db (:account_id res))
                   media (get-media-attrs db status-id)
                   tags (get-tags db status-id)]
        (->
          res
          (assoc :account account)
          (assoc :tags tags)
          (assoc :media media))))))

(defquery get-ids-by-thread-id
  [thread-id] [:id]
  "select id from statuses where thread_id = ? order by thread_depth")

(defn get-thread
  [db status-id]
  (d/let-flow [ids (get-ids-by-thread-id db
                    #(mapv :id %)
                    (:thread_id (partial get-status db)))]
    (->>
      ids
      (map (partial get-status db))
      (apply d/zip)
      (filter #(not (= % :deleted)))
      (vec))))

(defquery status-tags
  [status-id] [:tag]
  "select tag from status_tags where status_id = ?"
  #(mapv :tag %))
