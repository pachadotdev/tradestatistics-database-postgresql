lapply(
  c("archive", "data.table", "readstata13", "RPostgres"),
  function(x) {
    if (!requireNamespace(x, quietly = TRUE)) install.packages(x, repos = "https://cran.r-project.org")
  }
)

library(archive)
library(data.table)
library(readstata13)
library(RPostgres)

finp <- "finp/"

urls <- "https://www.lebow.drexel.edu/sites/default/files/2025-01/gsdb_v4.zip"

finp <- "finp/"

zips <- gsub(".*/", finp, urls)

try(dir.create(finp, recursive = T))

lapply(
  seq_along(zips),
  function(x) {
    # x = 1
    if (!file.exists(zips[x])) {
      try(download.file(urls[x], zips[x], method = "curl", quiet = TRUE))
    }
  }
)

ptrn <- "GSDB_V4_dyadic.dta"

if (!length(list.files(finp, pattern = ptrn)) > 0) {
  lapply(
    seq_along(zips),
    function(x) {
      archive_extract(zips[x], dir = finp)
    }
  )
}

files <- list.files(finp, pattern = "GSDB_", full.names = TRUE)

files

gsdb_dyadic <- as.data.table(read.dta13(files[1]))
gsdb <- fread(files[2])

str(gsdb_dyadic)
gsdb_dyadic[ , year := format(year, "%Y")]

setnames(gsdb_dyadic, c("sanctioning_state_iso3", "sanctioned_state_iso3"), c("sanctioning_state_dynamic", "sanctioned_state_dynamic"))

str(gsdb)

gsdb_codes <- gsdb_dyadic[ , .(case_id, sanctioning_state_dynamic, sanctioning_state, sanctioned_state_dynamic, sanctioned_state)]

gsdb_codes[, case_id := strsplit(as.character(case_id), ",\\s*")]

gsdb_codes <- gsdb_codes[
  , .(case_id = trimws(unlist(case_id))),
  by = setdiff(names(gsdb_codes), "case_id")
]

gsdb_codes[ , case_id := as.integer(case_id)]

gsdb_codes <- unique(
  gsdb_codes[, .(case_id, sanctioning_state_dynamic, sanctioned_state_dynamic)],
  by = "case_id"
)

nrow(gsdb)
nrow(merge(gsdb, gsdb_codes, by = "case_id", all.x = FALSE, all.y = FALSE))

gsdb <- merge(gsdb, gsdb_codes, by = "case_id", all.x = FALSE, all.y = FALSE)

setcolorder(gsdb, c("sanctioning_state_dynamic", "sanctioned_state_dynamic"), after = "sanctioning_state")

gsdb_dyadic[ , `:=`(sanctioning_state = NULL, sanctioned_state = NULL)]
gsdb[ , `:=`(sanctioning_state = NULL, sanctioned_state = NULL)]

str(gsdb)

desc_trade_vals <- sort(unique(trimws(unlist(strsplit(gsdb$descr_trade, ",")))))
gsdb[, (paste0("desc_trade_", desc_trade_vals)) := lapply(desc_trade_vals, function(v) {
  as.integer(grepl(paste0("(^|,)", v, "(,|$)"), descr_trade))
})]

obj_vals <- sort(unique(trimws(unlist(strsplit(gsdb$objective, ",")))))
gsdb[, (paste0("obj_", obj_vals)) := lapply(obj_vals, function(v) {
  as.integer(grepl(paste0("(^|,)", v, "(,|$)"), objective))
})]

suc_vals <- sort(unique(trimws(unlist(strsplit(gsdb$success, ",")))))
gsdb[, (paste0("suc_", suc_vals)) := lapply(suc_vals, function(v) {
  as.integer(grepl(paste0("(^|,)", v, "(,|$)"), success))
})]

gsdb[ , `:=`(descr_trade = NULL, objective = NULL, success = NULL)]

str(gsdb_dyadic)

desc_trade_vals <- sort(unique(trimws(unlist(strsplit(gsdb_dyadic$descr_trade, ",")))))
gsdb_dyadic[, (paste0("desc_trade_", desc_trade_vals)) := lapply(desc_trade_vals, function(v) {
  as.integer(grepl(paste0("(^|,)", v, "(,|$)"), descr_trade))
})]

obj_vals <- sort(unique(trimws(unlist(strsplit(gsdb_dyadic$objective, ",")))))
gsdb_dyadic[, (paste0("obj_", obj_vals)) := lapply(obj_vals, function(v) {
  as.integer(grepl(paste0("(^|,)", v, "(,|$)"), objective))
})]

suc_vals <- sort(unique(trimws(unlist(strsplit(gsdb_dyadic$success, ",")))))
gsdb_dyadic[, (paste0("suc_", suc_vals)) := lapply(suc_vals, function(v) {
  as.integer(grepl(paste0("(^|,)", v, "(,|$)"), success))
})]

gsdb_dyadic[ , `:=`(descr_trade = NULL, objective = NULL, success = NULL)]

con <- dbConnect(
  Postgres(),
  user = Sys.getenv("LOCAL_SQL_USR"),
  password = Sys.getenv("LOCAL_SQL_PWD"),
  dbname = "tradestatistics",
  host = "localhost"
)

dgd_countries <- as.data.table(dbGetQuery(con, "select * from dgd_countries"))

gsdb_dyadic[dgd_countries, on = .(sanctioning_state_dynamic = dynamic_code),
            sanctioning_state_iso3 := i.iso3]

gsdb_dyadic[dgd_countries, on = .(sanctioned_state_dynamic = dynamic_code),
            sanctioned_state_iso3 := i.iso3]

setcolorder(gsdb_dyadic, c("sanctioning_state_iso3", "sanctioned_state_iso3"), before = "sanctioning_state_dynamic")

gsdb_dyadic[, year := as.integer(year)]

# dbWriteTable(con, "gsdb", gsdb, overwrite = TRUE)
dbWriteTable(con, "gsdb_dyadic", gsdb_dyadic, overwrite = TRUE)

dbDisconnect(con)
