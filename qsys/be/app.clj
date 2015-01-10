(ns qsys.be
  (:require [vertx.core :refer [config deploy-verticle deploy-module]]))

(let [cfg (config)]
  (deploy-verticle "qsys/be/api.clj"
    :config (:api cfg)))

