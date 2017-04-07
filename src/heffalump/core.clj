(ns heffalump.core
  (require
      [heffalump.config :as conf]
      [heffalump.app :as app]
      [heffalump.db :as db]))

(defn- main
  [config-fname & args]
  (let [config (conf/load-config config-fname)
        db-val (db/init config)]
    (app/run! db-val config)))
