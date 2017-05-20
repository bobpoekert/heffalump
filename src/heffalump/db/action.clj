(ns heffalump.db.action
  (:require [heffalump.db-utils :refer :all]
            [heffalump.db.core :refer :all]
            [heffalump.db.account :refer [get-account]]
            [manifold.deferred :as d]))

(add-table! :action_log
  [
    idcolumn
    [:created_at :int]
    [:source :int]
    [:target :int]
    [:verb :int]])

(add-index! :action_log :source)

(defprotocol Action
  (action-id [v])
  (insert-action! [v db]))

(defn- action-row
  [value-sym action-args action-type]
  (let [target-key (first (filter #(:target (meta %)) action-args))
        source (cond
                target-key nil
                (= (count action-args 2)) `(get ~value-sym ~(keyword (second action-args)))
                :else (throw (IllegalStateException. "actions must have two args or an explicit target")))
        target-key (cond
                    target-key target-key
                    (= (count action-args) 2) (first action-args)
                    :else (throw (IllegalStateException. "actions must have two args or an explicit target")))]
    {:created_at `(or (:created-at ~value-sym) (heffalump.db-utils/now))
     :verb action-type
     :source source
     :target `(get ~value-sym ~(keyword target-key))}))

(defmacro defactions
  [actions]
  (let [action-syms (map #(symbol (format "%sAction" (name %))) actions)
        action-ids (zipmap action-syms (range (count action-syms)))
        action-args (zipmap action-syms (map second acitons))
        action-inflaters (zipmap action-syms (for [a actions] (first (filter #(and (seq %) (= (first %) 'inflate))))))
        value-sym (gensym "val")]
    `(do
      ~@(for [s action-syms]
        `(defrecord ~s ~(conj (get action-args s) 'created-at)
          Action
          (action-id [v#] ~(get action-ids s))
          (insert-action! [~value-sym db#]
            (heffalump.db-utils/insert-row! db# :action_log ~(action-row value-sym (get action-args s) (get action-ids s))))))
      (defn inflate-log-action
        [db row]
        (case (:verb row)
          ~@(mapcat
            (fn [s]
              (let [args (get action-args s)
                    arg-syms (map #(gensym (name %)) args)
                    constructor-bodies (map rest (get action-inflaters s))]
                [(get action-ids s)
                 `(d/let-flow [v# (d/zip ~@constructor-bodies)] 
                    (apply ~(symbol (format "->%s" (name s))) v# (:created-at row)))]))
            action-syms)
          nil)))))
        
(defactions
  (Follow
    [follower followee]
    (inflate
      (get-account db (:source row))
      (get-account db (:target row))
  (RequestFriend
    [follower followee]
    (inflate
      (get-account db (:source row))
      (get-account db (:target row))))
  (Authorize
    [^{:target true} follow-request follower followee]
    (inflate
      (d/let-flow [follow-request (get-by-id db :follow_requests (:target row))]
        [follow-request
          (get-account db (:follower follow-request))
          (get-account db (:followee follow-request))])))
  (Reject
    [^{:target true} follow-request follower followee]
    (inflate
      (d/let-flow [follow-request (get-by-id db :follow_requests (:target row))]
        [follow-request 
          (get-account db (:follower follow-request))
          (get-account db (:followee follow-request))])))
  (Unfollow
    [follower followee]
    (inflate
      [(get-account db (:source row))
       (get-account db (:target row))]))
  (Block
    [blocker blockee]
    (inflate
      [(get-account db (:source row))
       (get-account db (:target row))]))
  (Unblock
    [blocker blockee]
    (inflate
      [(get-account db (:source row))
       (get-account db (:target row))]))
  (Share
    [^{:target true} status]
    (inflate
      [(get-status db (:target row))]))
  (Fav
    [faver status]
    (inflate
      [(get-account db (:source row))
       (get-status db (:target row))]))
  (Unfav
    [faver status]
    (inflate
      [(get-account db (:source row))
       (get-status db (:target row))])))

(defquery user-actions-query
  [user-id] [:target :verb]
  "select target, verb from action_log where source = ? order by created_at desc")

(defn user-actions
  [db user-id limit]
  (user-actions-query db
    (fn [rows]
      (-> rows
        (map #(inflate-user-action db (assoc % :source user-id)))
        (take limit)
        (doall)))
    user-id))
