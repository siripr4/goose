(ns goose.batch_test
  (:require [goose.utils :as u]
            [goose.brokers.redis.broker :as redis]
            [goose.client :as client]))

(comment (client/enqueue-wait-test {:broker     (redis/new-producer redis/default-opts)
                                    :queue      "default-queue"
                                    :retry-opts {:a 1
                                                 :b 2}}
                                   'prn
                                   [1 2 3]))
(def test-redis-producer (redis/new-producer redis/default-opts))
(def test-opts (assoc client/default-opts :broker test-redis-producer))
(def test-batch [{:opts           test-opts
                  :execute-fn-sym `prn
                  :args           ["foo" "bar"]}
                 {:opts           test-opts
                  :execute-fn-sym `prn
                  :args           ["123" "456"]}
                 {:opts           test-opts
                  :execute-fn-sym `prn
                  :args           ["qwe" "xyz"]}])

(client/perform-batch-defer test-batch)



(comment
  (:storage-locations
    :meta-of-init-job
    :jobs-in-wait-q)

  [:create-a-batch-job
   :add-to-init-job-metadata
   :add-to-wait-q-and-reference-init-job-or-batch-id])