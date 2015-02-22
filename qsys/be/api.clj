(ns be.qsys.tinyleaps.api
  (:refer-clojure :exclude [send])
  (:require [ vertx.core :as core ]
            [ vertx.eventbus :as eb ]
            [ postgres.async :refer :all ]
            [ cheshire.core :as json ]
            [ clojure.core [async :refer [<!!]] [memoize :as memo] [cache :as cache]] ))

(def address "be.qsys.tinyleaps.api")

(def db (open-db (:db (core/config))))

(extend-protocol IPgParameter 
  clojure.lang.IPersistentMap
  (to-pg-value [value]
    (.getBytes (json/generate-string value))))

(defmethod from-pg-value com.github.pgasync.impl.Oid/JSON [oid value]
  (json/parse-string (String. value)))

(defmulti asyncCall :action)

(defmethod asyncCall "travelinfo" [msg] 
  (let [pars (:pars msg)
	stmt (str "select t.name, t.period, t.countries, t.postcount "
		  "from travelblog.travelinfo t "
		  "where t.id=$1")]
    (first (<!! (<query! db [stmt (:travel pars)])))))

(defmethod asyncCall "countries" [msg] 
  (let [stmt "select * from travelblog.country" ]
    (<!! (<query! db [stmt]))))

(defmethod asyncCall "posts" [msg] 
  (let [pars (:pars msg)
        stmt (str "select p.postdate, p.comments, p.countries, p.title, p.summary, p.text, p.text_en, p.flickr "
		   "from travelblog.postdata p "
                   "where p.travel_id = $1 "
    		   "order by p.postdate desc "
                   "limit $2 "
                   "offset $3")]
    (<!! (<query! db [stmt (:travel pars) (:limit pars) (:offset pars)]))))

(defmethod asyncCall :default [_] "")

(def asyncCall-mem
  (memo/lu asyncCall 
           (cache/ttl-cache-factory {} :ttl 300000) 
           :lu/threshold 64))

(eb/on-message
  address
  (fn [msg]
    (let [res (asyncCall-mem msg)] 
      (eb/reply {:result res}))))

(core/on-stop (close-db! db))
