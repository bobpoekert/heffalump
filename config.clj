
(let [data-dir "~/.heffalump"]
  {
    :data-dir data-dir
    :db (sqlite3 {:db (str data-dir "/db.sqlite")})
    :port 8000
    :hostname "heffalump.zone"})
         
