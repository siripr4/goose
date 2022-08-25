(ns goose.brokers.redis.worker
  (:require
    [goose.brokers.redis.consumer :as redis-consumer]
    [goose.brokers.redis.heartbeat :as redis-heartbeat]
    [goose.brokers.redis.orphan-checker :as redis-orphan-checker]
    [goose.brokers.redis.retry :as redis-retry]
    [goose.brokers.redis.scheduler :as redis-scheduler]
    [goose.brokers.redis.statsd :as redis-statsd]
    [goose.defaults :as d]
    [goose.job :as job]
    [goose.statsd]
    [goose.utils :as u]

    [clojure.tools.logging :as log]
    [com.climate.claypoole :as cp])
  (:import
    [java.util.concurrent TimeUnit]))

(defn- internal-stop
  "Gracefully shuts down the worker threadpool."
  [{:keys [thread-pool internal-thread-pool graceful-shutdown-sec]
    :as   opts}]
  ; Set state of thread-pool to SHUTDOWN.
  (log/warn "Shutting down thread-pool...")
  (cp/shutdown thread-pool)
  (cp/shutdown internal-thread-pool)

  ; Interrupt scheduler to exit sleep & terminate thread-pool.
  ; If not interrupted, shutdown time will increase by
  ; max(graceful-shutdown-sec, sleep time)
  (cp/shutdown! internal-thread-pool)

  ; Give jobs executing grace time to complete.
  (log/warn "Awaiting executing jobs to complete.")

  (.awaitTermination
    thread-pool
    graceful-shutdown-sec
    TimeUnit/SECONDS)

  (redis-heartbeat/stop opts)

  ; Set state of thread-pool to STOP.
  (log/warn "Sending InterruptedException to close threads.")
  (cp/shutdown! thread-pool))

(defn- chain-middlewares
  [middlewares]
  (let [call (if middlewares
               (-> redis-consumer/execute-job (middlewares))
               redis-consumer/execute-job)]
    (-> call
        (goose.statsd/wrap-metrics)
        (job/wrap-latency)
        (redis-retry/wrap-failure))))

(defn start
  [{:keys [redis-conn threads statsd-opts queue
           error-service-cfg scheduler-polling-interval-sec
           middlewares graceful-shutdown-sec]}]
  (let [thread-pool (cp/threadpool threads)
        ; Internal threadpool for scheduler, orphan-checker & heartbeat.
        internal-thread-pool (cp/threadpool d/redis-internal-thread-pool-size)
        random-str (subs (str (random-uuid)) 24 36) ; Take last 12 chars of UUID.
        id (str queue ":" (u/hostname) ":" random-str)
        call (chain-middlewares middlewares)
        opts {:id                             id
              :thread-pool                    thread-pool
              :internal-thread-pool           internal-thread-pool
              :redis-conn                     redis-conn
              :call                           call
              :error-service-cfg              error-service-cfg
              :statsd-opts                    statsd-opts

              :process-set                    (str d/process-prefix queue)
              :prefixed-queue                 (d/prefix-queue queue)
              :in-progress-queue              (redis-consumer/preservation-queue id)

              :graceful-shutdown-sec          graceful-shutdown-sec
              :scheduler-polling-interval-sec scheduler-polling-interval-sec}]

    (cp/future internal-thread-pool (redis-statsd/run opts))
    (cp/future internal-thread-pool (redis-heartbeat/run opts))
    (cp/future internal-thread-pool (redis-scheduler/run opts))
    (cp/future internal-thread-pool (redis-orphan-checker/run opts))

    (dotimes [_ threads]
      (cp/future thread-pool (redis-consumer/run opts)))

    #(internal-stop opts)))