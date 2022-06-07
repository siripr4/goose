(ns goose.scheduler
  (:require
    [goose.defaults :as d]
    [goose.redis :as r]
    [goose.utils :as u]
    [goose.validations.scheduler :as v]

    [clojure.tools.logging :as log]))

(def schedule-queue (u/prefix-queue d/schedule-queue))

(defn set-schedule
  [opts {:keys [perform-at perform-in-sec]}]
  (v/validate-schedule perform-at perform-in-sec)
  (cond
    perform-at
    (assoc opts :schedule (u/epoch-time-ms perform-at))

    perform-in-sec
    (assoc opts :schedule (u/add-sec perform-in-sec))))

(defn- set-schedule-config
  [job schedule queue]
  (-> job
      (assoc :schedule schedule)
      (assoc-in [:dynamic-config :execution-queue] queue)))

(defn schedule-job
  [redis-conn schedule
   {:keys [queue] :as job}]
  (let [scheduled-job (set-schedule-config job schedule queue)]
    (if (< schedule (u/epoch-time-ms))
      (r/enqueue-front redis-conn queue scheduled-job)
      (r/enqueue-sorted-set redis-conn schedule-queue schedule scheduled-job))))

(defn- execution-queue
  [job]
  (get-in job [:dynamic-config :execution-queue]))

(defn run
  [{:keys [internal-thread-pool redis-conn
           scheduler-polling-interval-sec]}]
  (u/while-pool
    internal-thread-pool
    (log/info "Polling scheduled jobs...")
    (u/log-on-exceptions
      (if-let [jobs (r/scheduled-jobs-due-now redis-conn schedule-queue)]
        (r/enqueue-due-jobs-to-front
          redis-conn schedule-queue
          jobs (group-by execution-queue jobs))
        (Thread/sleep (* 1000 (+ (rand-int 3) scheduler-polling-interval-sec))))))
  (log/info "Stopped scheduler. Exiting gracefully..."))
