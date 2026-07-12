lapply(
  c("RPostgres"),
  function(x) {
    if (!requireNamespace(x, quietly = TRUE)) install.packages(x, repos = "https://cran.r-project.org")
  }
)

library(RPostgres)

con <- dbConnect(
  Postgres(),
  user = Sys.getenv("LOCAL_SQL_USR"),
  password = Sys.getenv("LOCAL_SQL_PWD"),
  dbname = "tradestatistics",
  host = "localhost"
)

# DGD ----

dbExecute(con, "CREATE INDEX dgd_year ON dgd (year)")
dbExecute(con, "CREATE INDEX dgd_iso3_o ON dgd (iso3_o)")
dbExecute(con, "CREATE INDEX dgd_iso3_d ON dgd (iso3_d)")
dbExecute(con, "CREATE INDEX dgd_iso3_dynamic_o ON dgd (iso3_dynamic_o)")
dbExecute(con, "CREATE INDEX dgd_iso3_dynamic_d ON dgd (iso3_dynamic_d)")
dbExecute(con, "CREATE INDEX dgd_region_id_o ON dgd (region_id_o)")
dbExecute(con, "CREATE INDEX dgd_region_id_d ON dgd (region_id_d)")

dbExecute(con, "ALTER TABLE dgd_countries ADD CONSTRAINT iso3_dynamic UNIQUE (iso3, dynamic_code)")

dbExecute(con, "ALTER TABLE dgd ADD CONSTRAINT countries_o
  FOREIGN KEY (iso3_o, iso3_dynamic_o) REFERENCES dgd_countries (iso3, dynamic_code)")

dbExecute(con, "ALTER TABLE dgd ADD CONSTRAINT countries_d
  FOREIGN KEY (iso3_d, iso3_dynamic_d) REFERENCES dgd_countries (iso3, dynamic_code)")

dbExecute(con, "ALTER TABLE dgd_regions ADD CONSTRAINT region UNIQUE (region_id)")

dbExecute(con, "ALTER TABLE dgd ADD CONSTRAINT region_o
  FOREIGN KEY (region_id_o) REFERENCES dgd_regions (region_id)")

dbExecute(con, "ALTER TABLE dgd ADD CONSTRAINT region_d
  FOREIGN KEY (region_id_d) REFERENCES dgd_regions (region_id)")

# GSDB dyadic ----

# dbExecute(con, "ALTER TABLE gsdb_dyadic ADD COLUMN sanctioning_state_iso3 TEXT")
# dbExecute(con, "ALTER TABLE gsdb_dyadic ADD COLUMN sanctioned_state_iso3 TEXT")

# dbExecute(con, "
#   UPDATE gsdb_dyadic
#   SET sanctioning_state_iso3 = d.iso3
#   FROM dgd_countries d
#   WHERE gsdb_dyadic.sanctioning_state_dynamic = d.dynamic_code
# ")

# dbExecute(con, "
#   UPDATE gsdb_dyadic
#   SET sanctioned_state_iso3 = d.iso3
#   FROM dgd_countries d
#   WHERE gsdb_dyadic.sanctioned_state_dynamic = d.dynamic_code
# ")

dbExecute(con, "ALTER TABLE gsdb_dyadic ADD CONSTRAINT sanctioning_countries
  FOREIGN KEY (sanctioning_state_iso3, sanctioning_state_dynamic) REFERENCES dgd_countries (iso3, dynamic_code)")

dbExecute(con, "ALTER TABLE gsdb_dyadic ADD CONSTRAINT sanctioned_countries
  FOREIGN KEY (sanctioned_state_iso3, sanctioned_state_dynamic) REFERENCES dgd_countries (iso3, dynamic_code)")

dbExecute(con, "CREATE INDEX gsdb_dyadic_year ON gsdb_dyadic (year)")
dbExecute(con, "CREATE INDEX gsdb_dyadic_sanctioning_state_iso3 ON gsdb_dyadic (sanctioning_state_iso3)")
dbExecute(con, "CREATE INDEX gsdb_dyadic_sanctioned_state_iso3 ON gsdb_dyadic (sanctioned_state_iso3)")
dbExecute(con, "CREATE INDEX gsdb_dyadic_sanctioning_state_dynamic ON gsdb_dyadic (sanctioning_state_dynamic)")
dbExecute(con, "CREATE INDEX gsdb_dyadic_sanctioned_state_dynamic ON gsdb_dyadic (sanctioned_state_dynamic)")

# ITPDE ----

dbExecute(con, "ALTER TABLE itpd_industries ADD CONSTRAINT industries UNIQUE (industry_id)")
dbExecute(con, "ALTER TABLE itpd_sectors ADD CONSTRAINT sectors UNIQUE (broad_sector_id)")

dbExecute(con, "ALTER TABLE itpde ADD CONSTRAINT industries
  FOREIGN KEY (industry_id) REFERENCES itpd_industries (industry_id)")

dbExecute(con, "ALTER TABLE itpde ADD CONSTRAINT sectors
  FOREIGN KEY (broad_sector_id) REFERENCES itpd_sectors (broad_sector_id)")

dbExecute(con, "ALTER TABLE itpde ADD CONSTRAINT importers
  FOREIGN KEY (importer_iso3, importer_iso3_dynamic) REFERENCES dgd_countries (iso3, dynamic_code)")

dbExecute(con, "ALTER TABLE itpde ADD CONSTRAINT exporters
  FOREIGN KEY (exporter_iso3, exporter_iso3_dynamic) REFERENCES dgd_countries (iso3, dynamic_code)")

dbExecute(con, "CREATE INDEX itpde_year ON itpde (year)")
dbExecute(con, "CREATE INDEX itpde_importer_iso3 ON itpde (importer_iso3)")
dbExecute(con, "CREATE INDEX itpde_exporter_iso3 ON itpde (exporter_iso3)")
dbExecute(con, "CREATE INDEX itpde_importer_iso3_dynamic ON itpde (importer_iso3_dynamic)")
dbExecute(con, "CREATE INDEX itpde_exporter_iso3_dynamic ON itpde (exporter_iso3_dynamic)")

# ITPDS ----

dbExecute(con, "ALTER TABLE itpds ADD CONSTRAINT industries
  FOREIGN KEY (industry_id) REFERENCES itpd_industries (industry_id)")

dbExecute(con, "ALTER TABLE itpds ADD CONSTRAINT sectors
  FOREIGN KEY (broad_sector_id) REFERENCES itpd_sectors (broad_sector_id)")

dbExecute(con, "ALTER TABLE itpds ADD CONSTRAINT importers
  FOREIGN KEY (importer_iso3, importer_iso3_dynamic) REFERENCES dgd_countries (iso3, dynamic_code)")

dbExecute(con, "ALTER TABLE itpds ADD CONSTRAINT exporters
  FOREIGN KEY (exporter_iso3, exporter_iso3_dynamic) REFERENCES dgd_countries (iso3, dynamic_code)")

dbExecute(con, "CREATE INDEX itpds_year ON itpds (year)")
dbExecute(con, "CREATE INDEX itpds_importer_iso3 ON itpds (importer_iso3)")
dbExecute(con, "CREATE INDEX itpds_exporter_iso3 ON itpds (exporter_iso3)")
dbExecute(con, "CREATE INDEX itpds_importer_iso3_dynamic ON itpds (importer_iso3_dynamic)")
dbExecute(con, "CREATE INDEX itpds_exporter_iso3_dynamic ON itpds (exporter_iso3_dynamic)")

dbDisconnect(con)
