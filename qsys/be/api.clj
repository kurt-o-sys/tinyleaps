(ns be.qsys.tinyleaps.api
  (:refer-clojure :exclude [send])
  (:require [ vertx.core :as core ]
            [ vertx.eventbus :as eb ]
            [ postgres.async :refer :all ]
            [ clojure.core [async :refer [<!!]] [memoize :as memo] [cache :as cache]]
            ))


(def address "be.qsys.tinyleaps.api")

(def db (open-db {:hostname "localhost"
                  :port 5432
                  :database "main"
                  :username "kurt"
                  :password "oebele"
                  :pool-size 2 }))


(defmulti asyncCall :action)

(defmethod asyncCall "travelinfo" [msg] 
  (let [pars (:pars msg)
        stmt (str "select ti.name as name, "
                  "extract(epoch from ti.startdate) as startdate, "
                  "extract(epoch from ti.enddate) as enddate, "
                  "array_to_string(ti.countries,',') as countries "
                  "from travelblog.travelinfo ti "
                  "where ti.id = $1")]
    (<!! (<query! db [stmt (:travel pars)]))))

(defmethod asyncCall "countries" [msg] 
  (let [stmt "select * from travelblog.country" ]
    (<!! (<query! db [stmt]))))

(defmethod asyncCall "blogs" [msg] 
  (let [pars (:pars msg)
        stmt (str "select extract(epoch from bi.postdate), bi.title, bi.summary, bi.text "
                  "from travelblog.blogitem bi "
                  "where bi.id in "
                  "(select distinct r.blogitem"
                  " from travelblog.route r"
                  " where r.travel = $1 and r.blogitem is not null) "
                  "order by bi.postdate desc "
                  "limit $2 "
                  "offset $3")] 
    (<!! (<query! db [stmt (:travel pars) (:limit pars) (:offset pars)]))))

(defmethod asyncCall :default [_] "")


(def asyncCall° 
  (memo/lu asyncCall 
           (cache/ttl-cache-factory {} :300000) 
           :lu/treshold 64))

(eb/on-message
  address
  (fn [msg]
    (let [res (asyncCall° msg)] 
      (eb/reply {:result res}))))

(core/on-stop (close-db! db))
