(ns be.qsys.tinyleaps.api
  (:refer-clojure :exclude [send])
  (:require [ vertx.eventbus :as eb]
            [ clojure.core.async :refer [go chan put! <!]]))

(def address "be.qsys.tinyleaps.api")

(defmulti jdbcCall :action)

(defmethod jdbcCall "travelinfo" [msg] 
  (let [pars (:pars msg)
        stmt (str "select ti.name, extract(epoch from ti.startdate), extract(epoch from ti.enddate), array_to_json(ti.countries) "
                  "from travelblog.travelinfo ti "
                  "where ti.id = ?")]
    {:action "select"
     :stmt stmt
     :values [[(:travel pars)]] }
    ))


(defmethod jdbcCall "countries" [msg] 
  (let [stmt "select * from travelblog.country" ]
    {:action "select" 
     :stmt stmt}
    ))

(defmethod jdbcCall "blogs" [msg] 
  (let [pars (:pars msg)
        stmt (str "select extract(epoch from bi.postdate), bi.title, bi.summary, bi.text "
                  "from travelblog.blogitem bi "
                  "where bi.id in "
                  "(select distinct r.blogitem"
                  " from travelblog.route r"
                  " where r.travel = ? and r.blogitem is not null) "
                  "order by bi.postdate desc "
                  "limit ? "
                  "offset ?")] 
    {:action "select" 
     :stmt stmt
     :values [[(:travel pars) , (:limit pars), (:offset pars)]]}
    ))

(defmethod jdbcCall :default [_] "")

(defn send [addr msg]
  (let [ch (chan 1)]
    (eb/send addr msg #(put! ch %)) ch))

(eb/on-message
  address
  (fn [msg]
    (go (let [reply (<! (send "be.qsys.tinyleaps.postgresql" (jdbcCall msg)))]
          (eb/reply reply)))))

