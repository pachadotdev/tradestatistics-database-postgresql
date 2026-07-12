lapply(
  c("RPostgres", "data.table"),
  function(x) {
    if (!requireNamespace(x, quietly = TRUE)) install.packages(x, repos = "https://cran.r-project.org")
  }
)

library(RPostgres)
library(data.table)

con <- dbConnect(
  Postgres(),
  user = Sys.getenv("LOCAL_SQL_USR"),
  password = Sys.getenv("LOCAL_SQL_PWD"),
  dbname = "tradestatistics",
  host = "localhost"
)

yrs <- sort(as.integer(unlist(dbGetQuery(con, sprintf("select distinct year from itpde")))))

tbls <- dbListTables(con)

lapply(
  tbls,
  function(d) {
    print(d)

    if (d %in% c("itpde", "itpds")) {
      lapply(
        yrs,
        function(y) {
          fout <- sprintf("%s_%s.csv", d, y)
          if (file.exists(fout)) { return(TRUE) }
          chunk <- setDT(dbGetQuery(con, sprintf("select * from %s where year = %s", d, y)))
          fwrite(chunk, sprintf("fout/%s_%s.csv", d, y))
        }
      )
    } else {
      fout <- sprintf("fout/%s.csv", d)
      if (file.exists(fout)) { return(TRUE) }
      chunk <- setDT(dbGetQuery(con, sprintf("select * from %s", d)))
      fwrite(chunk, sprintf("fout/%s.csv", d))
    }
  }
)
