(ns goose.validations.client
  (:require
    [goose.utils :as u]
    [goose.validations.redis :refer [validate-redis]]
    [goose.validations.queue :refer [validate-queue]]
    [goose.validations.retry :refer [validate-retry]]
    [clojure.edn :as edn]))

(defn- args-unserializable?
  "Returns true if args are unserializable by edn.
  Pending BUG: https://github.com/nilenso/goose/issues/9"
  [args]
  (try
    (not (= args (edn/read-string (str args))))
    (catch Exception _
      true)))

(defn validate-async-params
  [redis-url redis-pool-opts
   queue schedule retry-opts execute-fn-sym args]
  (validate-redis redis-url redis-pool-opts)
  (validate-queue queue)
  (validate-retry retry-opts)
  (when-let
    [validation-error
     (cond
       (not (qualified-symbol? execute-fn-sym))
       ["execute-fn-sym should be qualified" (u/wrap-error :unqualified-fn execute-fn-sym)]

       (not (resolve execute-fn-sym))
       ["execute-fn-sym should be resolvable" (u/wrap-error :unresolvable-fn execute-fn-sym)]

       (args-unserializable? args)
       ["args should be serializable" (u/wrap-error :unserializable-args args)]

       (when schedule (not (int? schedule)))
       [":schedule should be an integer denoting epoch in milliseconds" (u/wrap-error :schedule-invalid schedule)])]
    (throw (apply ex-info validation-error))))
