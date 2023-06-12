(ns goose.batch
  (:require [goose.utils :as u]
            [goose.brokers.redis.broker :as redis]
            [goose.client :as client]))

(defn new
  [execute-fn-sym args queue ready-queue wait-queue retry-opts]
  {:id             (str (random-uuid))
   :execute-fn-sym execute-fn-sym
   :args           args
   ;; Since ready-queue is an internal implementation detail,
   ;; we store queue as well for find-by-pattern API queries.
   :queue          queue
   :ready-queue    ready-queue
   :retry-opts     retry-opts
   :enqueued-at    (u/epoch-time-ms)})

;(comment
;  "" init job will have to wait until it receives a hook for completion
;  - split init job into
;  - `create-batch`
;          - create a batch job, add it to `:wait-queue`
;          - `exec-fn-sym` of batch job
;  - move all sub jobs from `:wait-queue` to `:ready-queue`
;          - or sub-jobs are created on `:ready-queue` directly
;  - `add-jobs-to-batch`
;          - check batch state
;  - `created?` - add jobs
;  - `ready-for-exec?` - don't add jobs, return error
;  - `mark-batch-ready`
;          - batch-status -> `:ready-for-exec`
;          - don't split
;  )

(client/enqueue-wait-test {:broker     (redis/new-producer redis/default-opts)
                    :queue      "default-queue"
                    :retry-opts {:a 1
                                 :b 2}}
                   'prn
                   [1 2 3])




(comment
  (storage-locations
    :meta-of-init-job
    :jobs-in-wait-q)

  [:create-a-batch-job
   :add-to-init-job-metadata
   :add-to-wait-q-and-reference-init-job-or-batch-id])

