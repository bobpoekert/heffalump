(ns heffalump.db
  (import [com.google.common.cache CacheBuilder])
  (require [korma.core :as kc]))

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
