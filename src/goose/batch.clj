(ns goose.batch
  (:require [goose.utils :as u]))

(defn new
  ([]
   {:id          (str (random-uuid))
    :enqueued-at (u/epoch-time-ms)})
  ([_deferred?]
   {:id (str (random-uuid))}))



