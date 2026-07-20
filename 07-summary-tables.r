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

tbl <- "itpds"

yrs <- sort(as.integer(unlist(dbGetQuery(con, sprintf("select distinct year from %s", tbl)))))

lapply(
  yrs,
  function(y) {
    # y = yrs[1]
    print(y)

    chunk <- as.data.table(dbGetQuery(con, sprintf("select * from %s where year = %s and importer_iso3_dynamic != exporter_iso3_dynamic", tbl, y)))
    
    chunk2 <- chunk[, .(trade = sum(trade, na.rm = TRUE)), keyby = .(year, importer_iso3_dynamic)]
    dbWriteTable(con, paste0(tbl, "_imp"), chunk2, append = TRUE)

    chunk2e <- chunk[, .(trade = sum(trade, na.rm = TRUE)), keyby = .(year, exporter_iso3_dynamic)]
    dbWriteTable(con, paste0(tbl, "_exp"), chunk2e, append = TRUE)

    chunk3 <- chunk[, .(trade = sum(trade, na.rm = TRUE)), keyby = .(year, importer_iso3_dynamic, exporter_iso3_dynamic)]
    dbWriteTable(con, paste0(tbl, "_imp_exp"), chunk3, append = TRUE)

    chunk4 <- chunk[, .(trade = sum(trade, na.rm = TRUE)), keyby = .(year, importer_iso3_dynamic, exporter_iso3_dynamic, broad_sector_id)]
    dbWriteTable(con, paste0(tbl, "_imp_exp_sec"), chunk4, append = TRUE)

    chunk5 <- chunk[, .(trade = sum(trade, na.rm = TRUE)), keyby = .(year, broad_sector_id)]
    dbWriteTable(con, paste0(tbl, "_sec"), chunk5, append = TRUE)

    chunk6 <- chunk[, .(trade = sum(trade, na.rm = TRUE)), keyby = .(year, industry_id)]
    dbWriteTable(con, paste0(tbl, "_ind"), chunk6, append = TRUE)

    chunk7 <- chunk[, .(trade = sum(trade, na.rm = TRUE)), keyby = .(year, importer_iso3_dynamic, broad_sector_id)]
    dbWriteTable(con, paste0(tbl, "_imp_sec"), chunk7, append = TRUE)

    chunk7e <- chunk[, .(trade = sum(trade, na.rm = TRUE)), keyby = .(year, exporter_iso3_dynamic, broad_sector_id)]
    dbWriteTable(con, paste0(tbl, "_exp_sec"), chunk7e, append = TRUE)

    chunk8 <- chunk[, .(trade = sum(trade, na.rm = TRUE)), keyby = .(year, importer_iso3_dynamic, industry_id)]
    dbWriteTable(con, paste0(tbl, "_imp_ind"), chunk8, append = TRUE)

    chunk8e <- chunk[, .(trade = sum(trade, na.rm = TRUE)), keyby = .(year, exporter_iso3_dynamic, industry_id)]
    dbWriteTable(con, paste0(tbl, "_exp_ind"), chunk8e, append = TRUE)
  }
)

dbDisconnect(con)
