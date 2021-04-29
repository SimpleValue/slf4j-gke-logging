(ns sv.slf4j-gke-logging.core
  (:require [clojure.data.json :as json]))

(def date-format
  (doto (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    (.setTimeZone (java.util.TimeZone/getTimeZone "UTC"))
    ))

(defn prepare
  "https://cloud.google.com/logging/docs/structured-logging#special-payload-fields"
  [log-message]
  {:severity (get log-message
                  "level")
   :message (get log-message
                 "msg"
                 "")
   :times (.format date-format
                   (get log-message
                        "timestamp"))})

(defn print!
  [^java.io.PrintStream print-stream log-message]
  (.println print-stream
            (json/write-str (prepare log-message))))

(defn print-loop
  [{:keys [print-stream]}]
  (let [queue (de.simplevalue.slf4j.queue.QueueLogger/getQueue)]
    (loop []
      (let [log-message (.take queue)]
        (print! (or print-stream
                    System/err)
                log-message))
      (recur))))

(defn start!
  [{:keys [print-stream] :as params}]
  (let [thread (Thread. (fn []
                          (print-loop params)))]
    (.start thread)))

(comment

  (.info (org.slf4j.LoggerFactory/getLogger "test")
         "a message")

  (print! System/out
          (.take
           (de.simplevalue.slf4j.queue.QueueLogger/getQueue)
           ))

  (start! {})
  )
