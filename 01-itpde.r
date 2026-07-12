lapply(
  c("archive", "data.table", "RPostgres"),
  function(x) {
    if (!requireNamespace(x, quietly = TRUE)) install.packages(x, repos = "https://cran.r-project.org")
  }
)

library(archive)
library(data.table)
library(RPostgres)

finp <- "finp/"

urls <- "https://www.usitc.gov/data/gravity/itpd_e/r03/ITPDE_R03.zip"

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

ptrn <- "ITPDE_R03.csv"

if (!length(list.files(finp, pattern = ptrn)) > 0) {
  lapply(
    seq_along(zips),
    function(x) {
      archive_extract(zips[x], dir = finp)
    }
  )
}

csvs <- list.files(finp, pattern = ptrn, full.names = TRUE)

csvs

con <- dbConnect(
  Postgres(),
  user = Sys.getenv("LOCAL_SQL_USR"),
  password = Sys.getenv("LOCAL_SQL_PWD"),
  dbname = "tradestatistics",
  host = "localhost"
)

chunk_size <- 2500000
current_skip <- 1
continue_reading <- TRUE

all_cols <- colnames(fread(csvs, nrows = 1))

while (continue_reading) {
  chunk <- tryCatch(
    fread(csvs, skip = current_skip, nrows = chunk_size),
    error = function(e) NULL
  )

  if (!is.null(chunk)) {
    colnames(chunk) <- all_cols
  }

  if (is.null(chunk) || nrow(chunk) == 0) {
    continue_reading <- FALSE
    break
  }
  
  dbWriteTable(con, "itpde_tmp", chunk, append = TRUE)

  current_skip <- current_skip + chunk_size

  rm(chunk)
}

yrs <- as.data.table(dbGetQuery(con, "select distinct year from itpde_tmp"))
yrs <- sort(as.integer(yrs$year))

lapply(
  yrs,
  function(y) {
    # y = 1986

    message(y)

    tbls <- dbListTables(con)

    if (!any("itpd_sectors" %in% tbls)) {
      itpd_sectors <- as.data.table(dbGetQuery(con, "select distinct broad_sector from itpde_tmp"))

      setorder(itpd_sectors, broad_sector)

      itpd_sectors[, broad_sector_id := seq_len(nrow(itpd_sectors))]

      dbWriteTable(con, "itpd_sectors", itpd_sectors, overwrite = TRUE)
    } else {
      itpd_sectors <- as.data.table(dbGetQuery(con, "select * from itpd_sectors"))
    }

    if (!any("itpd_industries" %in% tbls)) {
      itpd_industries <- as.data.table(dbGetQuery(con, "select distinct industry_descr, industry_id from itpde_tmp"))
      
      setorder(itpd_industries, industry_id)

      itpd_industries[, industry_descr := gsub("^[0-9][0-9][0-9]\\s+", "", industry_descr)]

      dbWriteTable(con, "itpd_industries", itpd_industries, overwrite = TRUE)
    } else {
      itpd_industries <- as.data.table(dbGetQuery(con, "select * from itpd_industries"))
    }

    chunk <- as.data.table(dbGetQuery(con,
      sprintf("select * from itpde_tmp where year = %s", y)))

    chunk[, importer_name := NULL]
    chunk[, exporter_name := NULL]

    chunk <- merge(
      chunk,
      itpd_sectors,
      all.x = FALSE,
      all.y = FALSE
    )

    chunk[, broad_sector := NULL]

    chunk[, industry_descr := NULL]

    cls <- c("year", "importer_iso3", "importer_iso3_dynamic",  "exporter_iso3", "exporter_iso3_dynamic", 
            "broad_sector_id", "industry_id",  "flag_mirror", "flag_zero", "trade")

    setcolorder(chunk, cls)

    setorder(chunk, year, importer_iso3, exporter_iso3, broad_sector_id, industry_id)

    dbWriteTable(con, "itpde", chunk, append = TRUE)

    rm(chunk)
    gc()
  }
)

dbSendQuery(con, "drop table itpde_tmp")

dbDisconnect(con)
