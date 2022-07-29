(ns goose.heartbeat
  {:no-doc true}
  (:require
    [goose.brokers.redis.commands :as redis-cmds]
    [goose.defaults :as d]
    [goose.utils :as u]))

(defn heartbeat-id
  [id]
  (str d/heartbeat-prefix id))

(defn alive?
  [redis-conn id]
  (boolean (redis-cmds/get-key redis-conn (heartbeat-id id))))

(defn process-count
  [redis-conn process-set]
  (redis-cmds/set-size redis-conn process-set))

(defn run
  [{:keys [internal-thread-pool
           id redis-conn process-set
           graceful-shutdown-sec]}]
  (redis-cmds/add-to-set redis-conn process-set id)
  (u/while-pool
    internal-thread-pool
    ; Goose stops sending heartbeat when shutdown is initialized.
    ; Set expiry beyond graceful-shutdown time so in-progress jobs
    ; aren't considered abandoned and double executions are avoided.
    (let [expiry (max d/heartbeat-expire-sec graceful-shutdown-sec)]
      (redis-cmds/set-key-val redis-conn (heartbeat-id id) "alive" expiry)
      (Thread/sleep (* 1000 d/heartbeat-sleep-sec)))))

(defn stop
  [{:keys [id redis-conn process-set in-progress-queue]}]
  (redis-cmds/del-keys redis-conn [(str d/heartbeat-prefix id)])
  ; If job has swallowed thread-interrupted exception,
  ; don't del the process from the queue.
  ; Job/process get recovered/cleaned-up by orphan-checker.
  (when (= 0 (redis-cmds/list-size redis-conn in-progress-queue))
    (redis-cmds/del-from-set redis-conn process-set id)))
