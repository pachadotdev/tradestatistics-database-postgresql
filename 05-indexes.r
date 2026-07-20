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

# Drop dgd/dgd_countries rows not used in itpde/itpds ----

countries_in_use <- setDT(dbGetQuery(con, "
  SELECT DISTINCT ON (importer_iso3_dynamic) 
         importer_iso3_dynamic
  FROM itpde_imp_exp
  ORDER BY importer_iso3_dynamic
"))

countries_in_use2 <- setDT(dbGetQuery(con, "
  SELECT DISTINCT ON (exporter_iso3_dynamic) 
         exporter_iso3_dynamic
  FROM itpde_imp_exp
  ORDER BY exporter_iso3_dynamic
"))

countries_in_use3 <- setDT(dbGetQuery(con, "
  SELECT DISTINCT ON (importer_iso3_dynamic) 
         importer_iso3_dynamic
  FROM itpds_imp_exp
  ORDER BY importer_iso3_dynamic
"))

countries_in_use4 <- setDT(dbGetQuery(con, "
  SELECT DISTINCT ON (exporter_iso3_dynamic) 
         exporter_iso3_dynamic
  FROM itpds_imp_exp
  ORDER BY exporter_iso3_dynamic
"))

countries_in_use <- unique(c(
  unlist(countries_in_use),
  unlist(countries_in_use2),
  unlist(countries_in_use3),
  unlist(countries_in_use4)
))

codes_in_use <- paste(dbQuoteLiteral(con, countries_in_use), collapse = ", ")

# dgd rows must go first: dgd has FK constraints (countries_o, countries_d)
# pointing at dgd_countries, so dgd_countries rows can't be dropped while
# still referenced by dgd
dbExecute(con, sprintf("
  DELETE FROM dgd
  WHERE iso3_dynamic_o NOT IN (%s)
     OR iso3_dynamic_d NOT IN (%s)
", codes_in_use, codes_in_use))

# drop cases not in use in dgd_colours
dbExecute(con, sprintf("
  DELETE FROM dgd_colours
  WHERE iso3_dynamic NOT IN (%s)
", codes_in_use))

# drop cases not in use in gsdb_dyadic (its FK constraints also reference
# dgd_countries, so this must run before dgd_countries rows are dropped)
dbExecute(con, sprintf("
  DELETE FROM gsdb_dyadic
  WHERE sanctioning_state_dynamic NOT IN (%s)
     OR sanctioned_state_dynamic NOT IN (%s)
", codes_in_use, codes_in_use))

# now the orphaned dgd_countries rows can be safely removed
dbExecute(con, sprintf("
  DELETE FROM dgd_countries
  WHERE dynamic_code NOT IN (%s)
", codes_in_use))

# Countries that need deambiguation ----

dups <- dbGetQuery(con, "SELECT 
    country, 
    COUNT(*) AS n
FROM 
    dgd_countries
GROUP BY 
    country")

dups[dups$n > 1, ]

# > dups[dups$n > 1, ]
#          country n
# 21  Saudi Arabia 2
# 51        Serbia 2
# 92  South Africa 2
# 118        Sudan 2
# 231      Romania 2

# Saudi Arabia	SAU	SAU
# Saudi Arabia	SAU	SAU.X -> audi-Iraqi Neutral Zone was formerly known using ISO 3-alpha “NTZ”. It was discontinued at the end of 1992. Saudi Arabia without the Neutral Zone is coded using SAU.X.
# Serbia	      SRB	SRB
# Serbia	      SRB	SRB.X -> Serbia is coded with SRB.X following Kosovo’s split in 2008
# South Africa	ZAF	ZAF
# South Africa	ZAF	ZAF.X -> Namibia became independent of South Africa in 1990. South Africa code changed from ZAF to ZAF.X.
# Sudan       	SDN	SDN
# Sudan	        SDN	SDN.X -> Following the independence of South Sudan from Sudan in 2011, Sudan’s code changed from SDN to SDN.X
# Romania	      ROU	ROM
# Romania	      ROU	ROU -> Officially recognized ISO 3-alpha for Romania was ROM in 1948–2001, changing to ROU in 2002. Dynamic code for Romania reflects this change, while ISO 3-alpha remains ROU throughout to facilitate matching between this and other datasets.

# disambiguate remaining duplicated names with a short descriptive suffix
dbExecute(con, "
  UPDATE dgd_countries
  SET country = CASE dynamic_code
    WHEN 'SAU.X' THEN 'Saudi Arabia (after Neutral Zone discontinued)'
    WHEN 'SRB.X' THEN 'Serbia (after Kosovo split)'
    WHEN 'ZAF.X' THEN 'South Africa (after Namibia independence)'
    WHEN 'SDN.X' THEN 'Sudan (after South Sudan independence)'
    WHEN 'ROM'   THEN 'Romania (ISO 3-alpha ROM, pre-2002)'
    WHEN 'ROU'   THEN 'Romania (ISO 3-alpha ROU, 2002+)'
    ELSE country
  END
  WHERE dynamic_code IN ('SAU.X', 'SRB.X', 'ZAF.X', 'SDN.X', 'ROM', 'ROU')
")

dups <- dbGetQuery(con, "SELECT 
    country, 
    COUNT(*) AS n
FROM 
    dgd_countries
GROUP BY 
    country")

dups[dups$n > 1, ]

# MYS.Y is a typo, use MYS.X

# rename MYS.Y to MYS.X in place (keep it as MYS-MYS.X in dgd_countries,
# not add a separate row). This requires updating the parent row in
# dgd_countries and all FK-referencing child tables atomically, since
# neither can be updated first without violating the other's FK constraint.
# Temporarily make the FK constraints deferrable so they're only checked
# at COMMIT, once everything is consistent.
dbExecute(con, "ALTER TABLE dgd ALTER CONSTRAINT countries_o DEFERRABLE INITIALLY DEFERRED")
dbExecute(con, "ALTER TABLE dgd ALTER CONSTRAINT countries_d DEFERRABLE INITIALLY DEFERRED")
dbExecute(con, "ALTER TABLE itpde ALTER CONSTRAINT importers DEFERRABLE INITIALLY DEFERRED")
dbExecute(con, "ALTER TABLE itpde ALTER CONSTRAINT exporters DEFERRABLE INITIALLY DEFERRED")
dbExecute(con, "ALTER TABLE itpds ALTER CONSTRAINT importers DEFERRABLE INITIALLY DEFERRED")
dbExecute(con, "ALTER TABLE itpds ALTER CONSTRAINT exporters DEFERRABLE INITIALLY DEFERRED")
dbExecute(con, "ALTER TABLE gsdb_dyadic ALTER CONSTRAINT sanctioning_countries DEFERRABLE INITIALLY DEFERRED")
dbExecute(con, "ALTER TABLE gsdb_dyadic ALTER CONSTRAINT sanctioned_countries DEFERRABLE INITIALLY DEFERRED")

dbWithTransaction(con, {
  dbExecute(con, "UPDATE dgd_countries SET dynamic_code = 'MYS.X' WHERE dynamic_code = 'MYS.Y'")
  dbExecute(con, "UPDATE dgd SET iso3_dynamic_o = 'MYS.X' WHERE iso3_dynamic_o = 'MYS.Y'")
  dbExecute(con, "UPDATE dgd SET iso3_dynamic_d = 'MYS.X' WHERE iso3_dynamic_d = 'MYS.Y'")
  dbExecute(con, "UPDATE itpde SET importer_iso3_dynamic = 'MYS.X' WHERE importer_iso3_dynamic = 'MYS.Y'")
  dbExecute(con, "UPDATE itpde SET exporter_iso3_dynamic = 'MYS.X' WHERE exporter_iso3_dynamic = 'MYS.Y'")
  dbExecute(con, "UPDATE itpds SET importer_iso3_dynamic = 'MYS.X' WHERE importer_iso3_dynamic = 'MYS.Y'")
  dbExecute(con, "UPDATE itpds SET exporter_iso3_dynamic = 'MYS.X' WHERE exporter_iso3_dynamic = 'MYS.Y'")
  dbExecute(con, "UPDATE gsdb_dyadic SET sanctioning_state_dynamic = 'MYS.X' WHERE sanctioning_state_dynamic = 'MYS.Y'")
  dbExecute(con, "UPDATE gsdb_dyadic SET sanctioned_state_dynamic = 'MYS.X' WHERE sanctioned_state_dynamic = 'MYS.Y'")
})

# restore the constraints to their original (non-deferrable) behaviour
dbExecute(con, "ALTER TABLE dgd ALTER CONSTRAINT countries_o NOT DEFERRABLE")
dbExecute(con, "ALTER TABLE dgd ALTER CONSTRAINT countries_d NOT DEFERRABLE")
dbExecute(con, "ALTER TABLE itpde ALTER CONSTRAINT importers NOT DEFERRABLE")
dbExecute(con, "ALTER TABLE itpde ALTER CONSTRAINT exporters NOT DEFERRABLE")
dbExecute(con, "ALTER TABLE itpds ALTER CONSTRAINT importers NOT DEFERRABLE")
dbExecute(con, "ALTER TABLE itpds ALTER CONSTRAINT exporters NOT DEFERRABLE")
dbExecute(con, "ALTER TABLE gsdb_dyadic ALTER CONSTRAINT sanctioning_countries NOT DEFERRABLE")
dbExecute(con, "ALTER TABLE gsdb_dyadic ALTER CONSTRAINT sanctioned_countries NOT DEFERRABLE")

dbDisconnect(con)
