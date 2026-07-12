# nice palette
clrs <- c(
  "#74c0e2", "#406662", "#549e95", "#8abdb6", "#bcd8af", "#a8c380", "#ede788",
  "#d6c650", "#dc8e7a", "#d05555", "#bf3251", "#872a41", "#993f7b", "#7454a6",
  "#a17cb0", "#d1a1bc", "#a1aafb", "#5c57d9", "#1c26b3", "#4d6fd0", "#7485aa",
  "#d3d3d3"
)

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

clrs <- c("#d1a1bc", "#a1aafb", "#a8c380", "#d05555", "#a17cb0", "#d3d3d3")

dgd_countries <- as.data.table(dbGetQuery(con, "select distinct iso3_dynamic_o as iso3_dynamic, region_id_o as region_id from dgd"))
dgd_countries2 <- as.data.table(dbGetQuery(con, "select distinct iso3_dynamic_d as iso3_dynamic, region_id_d as region_id from dgd"))

dgd_countries <- rbind(dgd_countries, dgd_countries2)
dgd_countries <- unique(dgd_countries)

dgd_region <- as.data.table(dbGetQuery(con, "select * from dgd_regions"))

dgd_region[, region_colour := fcase(
  region_id == 1, clrs[6],
  region_id == 2, clrs[1],
  region_id %in% c(3L, 4L, 10L, 12L), clrs[2],
  region_id %in% c(5L, 6L, 7L, 9L, 13L, 14L), clrs[3],
  region_id == 8L, clrs[4],
  region_id == 11L, clrs[5],
  default = clrs[6]
)]

dgd_colours <- merge(dgd_countries, dgd_region)
dgd_colours <- dgd_colours[, .(iso3_dynamic, region_id, region_colour)]

dbWriteTable(con, "dgd_colours", dgd_colours, overwrite = TRUE)

clrs <- c("#74c0e2","#406662","#a17cb0","#d05555")

itpd_colours <- as.data.table(dbGetQuery(con, "select * from itpd_sectors"))
itpd_colours[, broad_sector := NULL]
itpd_colours[, colour := clrs[1:nrow(itpd_colours)]]

dbWriteTable(con, "itpd_colours", itpd_colours, overwrite = TRUE)
