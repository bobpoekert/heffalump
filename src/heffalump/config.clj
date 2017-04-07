(ns heffalump.config
  (require [clojure.java.io :as io])
  (use korma.db))

(defn load-config
  [fname]
  (with-open [fd (io/reader fname)]
    (eval (read-string (slurp fd)))))
