(ns sv.slf4j-gke-logging.core
  (:require [clojure.data.json :as json]))

(def date-format
  (doto (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    (.setTimeZone (java.util.TimeZone/getTimeZone "UTC"))
    ))

(defn stacktrace-str
  [throwable]
  (with-open [out (java.io.ByteArrayOutputStream.)
              print-writer (java.io.PrintWriter. out)]
    (.printStackTrace throwable
                      print-writer)
    (.flush print-writer)
    (String. (.toByteArray out)
             "UTF-8")))

(defn prepare
  "https://cloud.google.com/logging/docs/structured-logging#special-payload-fields"
  [log-message]
  {:severity (get log-message
                  "level")
   :textPayload (str (get log-message
                          "msg"
                          "")
                     (when-let [throwable (get log-message
                                               "throwable")]
                       (str "\n"
                            (stacktrace-str throwable))))
   :times (.format date-format
                   (get log-message
                        "timestamp"))})

(defn print!
  [^java.io.PrintStream print-stream log-message]
  (.println print-stream
            (json/write-str (prepare log-message))))

(defn log!
  [log-message]
  (print! System/err
          log-message))

(defn print-loop
  [{:keys [log!]}]
  (let [queue (de.simplevalue.slf4j.queue.QueueLogger/getQueue)]
    (loop []
      (let [log-message (.take queue)]
        (log! log-message))
      (recur))))

(defn start!
  [params]
  (let [thread (Thread. (fn []
                          (print-loop (merge
                                       {:log! log!}
                                       params))))]
    (.start thread)))

(comment

  (.info (org.slf4j.LoggerFactory/getLogger "test")
         "a message")

  (.error (org.slf4j.LoggerFactory/getLogger "test")
          "an error"
          (Exception.))

  (.debug (org.slf4j.LoggerFactory/getLogger "test")
          "debug")

  (print! System/out
          (.take
           (de.simplevalue.slf4j.queue.QueueLogger/getQueue)
           ))

  (start! {:log? (fn [log-message]
                   (not (#{"DEBUG" "TRACE"} (get log-message
                                                 "level"))))})
  )
