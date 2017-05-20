(ns heffalump.crawler
  (require [manifold.deferred :as d]
           [manifold.time :as mt]
           [durable-queue :as dq]
           [ostatus.http :as oh]
           [heffalump.db :as db]))

(defn get-queue
  [app-config]
  (dq/queues (str (:data-dir app-config) "/queue") {}))

(def job-handlers (atom {}))

(defmacro io-retries
  [body]
  `(loop [n# 0]
    (let [res# (try 
                ~@body
                (catch java.io.IOException e#
                  (if (< n# 100)
                    ::fail
                    (throw e#))))]
      (if (= res# ::fail)
        (recur (inc n#))
        res#))))

(defn process-queue
  [app queue max-concurrent-tasks]
  (let [in-progress (:in-progress (dq/stats queue))]
    (dotimes [i (max 0 (- max-concurrent-tasks in-progress))]
      (when-let [task (io-retries (dq/take! queue :omnibus))]
        (let [thunk (get @job-handlers (:job-key @task))]
          (if thunk
            (d/let-flow [result (apply thunk app queue (:args @task))]
              (io-retries
                (dq/complete! task))
              (process-queue app queue max-concurrent-tasks))
            (dq/complete! task)))))))

(defmacro defprocessor
  [& defn-stuff]
  (let [res (macroexpand (cons 'defn defn-stuff))
        sym (second res)]
    `(do
      (defn ~@defn-stuff)
      (swap! job-handlers #(assoc % ~(keyword sym) ~sym)))))

(defn start!
  [app max-concurrent-tasks]
  (mt/every (mt/seconds 1) (partial process-queue app (get-queue app) max-concurrnet-tasks)))

(defn enqueue!
  [queue job-key & args]
  (io-retries
    (dq/put! queue :omnibus {:job-key job-key :args args})))

(defprocessor account-crawl
  [queue app account]
  (let [dbc (:db app)
        account-info @account]
    (d/let-flow [account (db/get-or-createa-account-by-info! account-info)
                 freinds (oh/get-friends account-info)]
      (db/update-row! dbc :accounts {:id (:id account) :friends_crawled true})
      (d/let-flow [followers (apply d/zip (map db/get-account-by-info (:followers friends)))
                   following (apply d/zip (map db/get-account-by-info (:following freinds)))]
        (doseq [friend (concat followers following)]
          (when-not (:friends-crawled friend)
            (enqueue! queue :account-crawl friend)))
        (d/zip
          (apply d/zip (map #(db/add-follower! dbc (:id account) (:id %)) followers))
          (apply d/zip (map #(db/add-follower! dbc (:id %) (:id account)) following)))))))
