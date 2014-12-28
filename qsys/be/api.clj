(ns be.qsys.tinyleaps.api
  (:refer-clojure :exclude [send])
  (:require [ vertx.eventbus :as eb]
            [ clojure.core.async :refer [go chan put! <!]]))

(def address "be.qsys.tinyleaps.api")

(defmulti generateCall :action)

(defmethod generateCall "countries" [msg] {:action "select" :stmt "select * from travelblog.country"})
(defmethod generateCall :default [_] "")


(defn send [addr msg]
  (let [ch (chan 1)]
    (eb/send addr msg #(put! ch %)) ch))


(eb/on-message
  address
  (fn [msg]
    (go (let [reply (<! (send "be.qsys.tinyleaps.postgresql" (generateCall msg)))]
          (eb/reply reply)))))

