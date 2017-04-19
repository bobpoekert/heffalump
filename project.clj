(defproject heffalump "0.1.0-SNAPSHOT"
  :description "A federated social networking server"
  :license {:name "MIT License"
            :url "https://github.com/bobpoekert/heffalump/blob/master/LICENSE"}
  :java-source-paths ["src/java"]
  :dependencies [
    [org.clojure/clojure "1.8.0"]
    [aleph "0.4.3"]
    [cheshire "5.7.0"]
    [hiccup "1.0.5"]
    [org.clojure/data.xml "0.0.8"]
    [compojure "1.5.2"]
    [org.clojure/java.jdbc "0.7.0-alpha3"]
    [org.apache.derby/derby "10.13.1.1"]
    [com.google.guava/guava "21.0"]
    [net.sf.trove4j/trove4j "3.0.3"]
    [byte-streams "0.2.2"]
    [org.clojure/data.fressian "0.2.1"]
    [org.mindrot/jbcrypt "0.4"]
    [org.clojure/test.check "0.9.0"]
])
