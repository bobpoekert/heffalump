(ns heffalump.core
  (:require
      [heffalump.config :as conf]
      [heffalump.app :as app]
      [heffalump.db :as db]))

(defn- main
  [& args]
  (let [config (if (empty? args)
                conf/default-config
                (conf/load-config (first args)))
        db-val (db/init config)]
    (app/run! db-val config)))
