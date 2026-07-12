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

urls <- c(
  "https://www.usitc.gov/data/gravity/dgd_docs/release_2.1_1948_1999.zip",
  "https://www.usitc.gov/data/gravity/dgd_docs/release_2.1_2000_2019.zip"
)

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

ptrn <- "release_2.1_1948_1959.csv"

if (!length(list.files(finp, pattern = ptrn)) > 0) {
  lapply(
    seq_along(zips),
    function(x) {
      archive_extract(zips[x], dir = finp)
    }
  )
}

ptrn <- "release_2.1.*csv"

csvs <- list.files(finp, pattern = ptrn, full.names = TRUE)

csvs

con <- dbConnect(
  Postgres(),
  user = Sys.getenv("LOCAL_SQL_USR"),
  password = Sys.getenv("LOCAL_SQL_PWD"),
  dbname = "tradestatistics",
  host = "localhost"
)

# Usual processing ----

all_cols <- colnames(fread(csvs[1], nrows = 1))

dgd <- lapply(
  seq_along(csvs),
  function(x) {
    # x = 1
    fread(csvs[x])
  }
)

dgd <- rbindlist(dgd)

to_int <- c(
  "colony_of_destination_ever",
  "colony_of_origin_ever",
  "colony_ever",
  "common_colonizer",
  "common_legal_origin",
  "contiguity",
  "member_gatt_o",
  "member_wto_o",
  "member_eu_o",
  "member_gatt_d",
  "member_wto_d",
  "member_eu_d",
  "member_gatt_joint",
  "member_wto_joint",
  "member_eu_joint",
  "landlocked_o",
  "island_o",
  "landlocked_d",
  "island_d",
  "agree_pta_goods",
  "agree_pta_services",
  "agree_fta",
  "agree_eia",
  "agree_cu",
  "agree_psa",
  "agree_fta_eia",
  "agree_cu_eia",
  "agree_pta",
  "hostility_level_o",
  "hostility_level_d",
  "common_language",
  "polity_o",
  "polity_d",
  "sanction_threat",
  "sanction_threat_trade",
  "sanction_imposition",
  "sanction_imposition_trade"
)

dgd[, (to_int) := lapply(.SD, as.integer), .SDcols = to_int]

str(dgd)

# Move countries ----

dgd_countries <- dgd[ , .(country_o, iso3_o, dynamic_code_o)]
dgd_countries <- unique(dgd_countries)
colnames(dgd_countries) <- c("country", "iso3", "dynamic_code")

dgd_countries2 <- dgd[ , .(country_d, iso3_d, dynamic_code_d)]
dgd_countries2 <- unique(dgd_countries2)
colnames(dgd_countries2) <- c("country", "iso3", "dynamic_code")

dgd_countries <- rbind(dgd_countries, dgd_countries2)
dgd_countries <- unique(dgd_countries)

# Drop countries that are duplicate with their old names ----

# these gave problems in 05-indexes.r

# Samoa: Western Samoa and Samoa are the same country (Samoa is the official name)
# Malaysia: Malagasy Republic was a transistion name post independence
# Vatican City: It is the official name of the Holy See
# Saint Vincent and the Grenadines is the official name for Saint Vincent
# Panama Canal Zone: It is a deleted/historic ISO code that with and without "former" is "PCZ". It was an US unincorporated US territory added back to Panana in 1980
# Rhodesia: Became Zimbabwe
# Ceylon: became Sri Lanka
# Muskat and Oman: Renamed to Oman
# Jordan: Dropped the "Trans" prefix after a geographic expansion in 1948
# Cambodia: Khmer Republic and Kampuchea correspond to transitional renames after coups
# Spanish Sahara: Renamed to Western Sahara in 1975 name

# > dgd_countries[dynamic_code == "WSM", ]
#          country   iso3 dynamic_code
#           <char> <char>       <char>
# 1: Western Samoa    WSM          WSM
# 2:         Samoa    WSM          WSM

# > dgd_countries[dynamic_code == "MDG", ]
#              country   iso3 dynamic_code
#               <char> <char>       <char>
# 1: Malagasy Republic    MDG          MDG
# 2:        Madagascar    MDG          MDG

# > dgd_countries[dynamic_code == "VAT", ]
#         country   iso3 dynamic_code
#          <char> <char>       <char>
# 1: Vatican City    VAT          VAT
# 2:     Holy See    VAT          VAT

# > dgd_countries[dynamic_code == "VCT", ]
#                             country   iso3 dynamic_code
#                              <char> <char>       <char>
# 1:                    Saint Vincent    VCT          VCT
# 2: Saint Vincent and the Grenadines    VCT          VCT

# > dgd_countries[dynamic_code == "PCZ", ]
#                     country   iso3 dynamic_code
#                      <char> <char>       <char>
# 1: Former Panama Canal Zone    PCZ          PCZ
# 2:        Panama Canal Zone    PCZ          PCZ

# > dgd_countries[dynamic_code == "RHO", ]
#              country   iso3 dynamic_code
#               <char> <char>       <char>
# 1:          Rhodesia    RHO          RHO
# 2: Zimbabwe-Rhodesia    RHO          RHO

# > dgd_countries[dynamic_code == "LKA", ]
#      country   iso3 dynamic_code
#       <char> <char>       <char>
# 1:    Ceylon    LKA          LKA
# 2: Sri Lanka    LKA          LK

# > dgd_countries[dynamic_code == "OMN", ]
#            country   iso3 dynamic_code
#             <char> <char>       <char>
# 1: Muscat and Oman    OMN          OMN
# 2:            Oman    OMN          OMN

# > dgd_countries[dynamic_code == "JOR", ]
#        country   iso3 dynamic_code
#         <char> <char>       <char>
# 1: Transjordan    JOR          JOR
# 2:      Jordan    JOR          JOR

# > dgd_countries[dynamic_code == "KHM", ]
#           country   iso3 dynamic_code
#            <char> <char>       <char>
# 1:       Cambodia    KHM          KHM
# 2: Khmer Republic    KHM          KHM
# 3:      Kampuchea    KHM          KHM

# > dgd_countries[dynamic_code == "ESH", ]
#           country   iso3 dynamic_code
#            <char> <char>       <char>
# 1: Spanish Sahara    ESH          ESH
# 2: Western Sahara    ESH          ESH

# dgd_countries[country == "Spanish Sahara", ]
# dgd_countries[dynamic_code == "ESH", ]

dgd_countries <- dgd_countries[country != "Western Samoa", ]
dgd_countries <- dgd_countries[country != "Malagasy Republic", ]
dgd_countries <- dgd_countries[country != "Holy See", ]
dgd_countries <- dgd_countries[country != "Saint Vincent", ]
dgd_countries <- dgd_countries[country != "Former Panama Canal Zone", ]
dgd_countries <- dgd_countries[country != "Rhodesia", ]
dgd_countries <- dgd_countries[country != "Ceylon", ]
dgd_countries <- dgd_countries[country != "Oman", ]
dgd_countries <- dgd_countries[country != "Transjordan", ]
dgd_countries <- dgd_countries[country != "Khmer Republic", ]
dgd_countries <- dgd_countries[country != "Kampuchea", ]
dgd_countries <- dgd_countries[country != "Spanish Sahara", ]

# Remove European Community - EUN12.X, it is a duplicate of EUN12
dgd_countries <- dgd_countries[!(country == "European Community" & dynamic_code == "EU12.X"), ]

dgd_countries[country == "European Community"]
dgd_countries[country == "European Union"]

setorder(dgd_countries, country)

dbWriteTable(con, "dgd_countries", dgd_countries, overwrite = TRUE)

dgd[ , country_o := NULL]
dgd[ , country_d := NULL]

# Move regions ----

dgd_regions <- unique(dgd[, .(region_o)])
colnames(dgd_regions) <- "region"

dgd_regions2 <- unique(dgd[, .(region_d)])
colnames(dgd_regions2) <- "region"

dgd_regions <- rbind(dgd_regions, dgd_regions2)
dgd_regions <- unique(dgd_regions[, .(region)])

setorder(dgd_regions, region)
dgd_regions[ , region_id := seq_len(nrow(dgd_regions))]

# Fixes duplicated label "south_east_asia" vs "suth_east_asia" in the usitc_region_names table ----

dgd[, region_o := fifelse(region_o == "suth_east_asia",  "south_east_asia", region_o)]
dgd[, region_d := fifelse(region_d == "suth_east_asia",  "south_east_asia", region_d)]

dgd_regions <- dgd_regions[region_id != 16L, ]
dgd_regions[, region := fifelse(region == "",  "not_applicable", region)]

dbWriteTable(con, "dgd_regions", dgd_regions, overwrite = TRUE)

colnames(dgd_regions) <- c("region_o", "region_id_o")
dgd <- merge(dgd, dgd_regions, all.x = FALSE, all.y = FALSE)

setkey(dgd, NULL)
colnames(dgd_regions) <- c("region_d", "region_id_d")
dgd <- merge(dgd, dgd_regions, all.x = FALSE, all.y = FALSE)

dgd[ , region_o := NULL]
dgd[ , region_d := NULL]

# Fix wrong colony_ever (symmetric) ----

# I found this a while back for the MA thesis, the thing persists

dgd[year == 2000L & iso3_o == "CHL" & iso3_d == "ESP", .(colony_of_origin_ever, colony_of_destination_ever, colony_ever)]
dgd[year == 2000L & iso3_o == "ESP" & iso3_d == "CHL", .(colony_of_origin_ever, colony_of_destination_ever, colony_ever)]

# > dgd[year == 2000L & iso3_o == "CHL" & iso3_d == "ESP", .(colony_of_origin_ever, colony_of_destination_ever, colony_ever)]
# dgd[year == 2000L & iso3_o == "ESP" & iso3_d == "CHL", .(colony_of_origin_ever, colony_of_destination_ever, colony_ever)]
#    colony_of_origin_ever colony_of_destination_ever colony_ever
#                    <num>                      <num>       <num>
# 1:                     0                          1           1

# > dgd[year == 2000L & iso3_o == "ESP" & iso3_d == "CHL", .(colony_of_origin_ever, colony_of_destination_ever, colony_ever)]
#    colony_of_origin_ever colony_of_destination_ever colony_ever
#                    <num>                      <num>       <num>
# 1:                     1                          0           1

dgd[year == 2000L & iso3_o == "CHL" & iso3_d == "ARG", .(common_colonizer)]
dgd[year == 2000L & iso3_o == "ARG" & iso3_d == "CHL", .(common_colonizer)]

# > dgd[year == 2000L & iso3_o == "CHL" & iso3_d == "ARG", .(common_colonizer)]
# dgd[year == 2000L & iso3_o == "ARG" & iso3_d == "CHL", .(common_colonizer)]
#    common_colonizer
#               <num>
# 1:                1

# > dgd[year == 2000L & iso3_o == "ARG" & iso3_d == "CHL", .(common_colonizer)]
#    common_colonizer
#               <num>
# 1:                0

# this can distort gravity estimates!
# think about a subset focused on Latin America

dgd[year == 2000L & iso3_o == "CHL" & iso3_d == "ARG", .(common_language)]
dgd[year == 2000L & iso3_o == "ARG" & iso3_d == "CHL", .(common_language)]

setnames(dgd, c("dynamic_code_o", "dynamic_code_d"), c("iso3_dynamic_o", "iso3_dynamic_d"))

dgd_colonizer <- dgd[, .(year, iso3_o, iso3_dynamic_o, iso3_d, iso3_dynamic_d, 
    common_colonizer)][, `:=`(iso3_min = pmin(iso3_o, iso3_d), 
    iso3_max = pmax(iso3_o, iso3_d), iso3_dynamic_min = pmin(iso3_dynamic_o, 
        iso3_dynamic_d), iso3_dynamic_max = pmax(iso3_dynamic_o, 
        iso3_dynamic_d))][, .(common_colonizer = max(common_colonizer, 
    na.rm = TRUE)), keyby = .(iso3_min, iso3_max, iso3_dynamic_min, 
    iso3_dynamic_max)]

dgd_colonizer[iso3_min == "ARG" & iso3_max == "CHL", ]

dgd[ , common_colonizer := NULL]

dgd <- dgd[, `:=`(iso3_min = pmin(iso3_o, iso3_d), iso3_max = pmax(iso3_o,
  iso3_d), iso3_dynamic_min = pmin(iso3_dynamic_o, iso3_dynamic_d),
  iso3_dynamic_max = pmax(iso3_dynamic_o, iso3_dynamic_d))][dgd_colonizer,
  on = .(iso3_min, iso3_max, iso3_dynamic_min, iso3_dynamic_max),
    nomatch = NULL, allow.cartesian = TRUE]

dgd[ , `:=`(iso3_min = NULL, iso3_max = NULL, iso3_dynamic_min = NULL, iso3_dynamic_max = NULL)]

setcolorder(dgd, "common_colonizer", after = "colony_ever")

# Common legal origin is also affected ----

dgd[year == 2000L & iso3_o == "CHL" & iso3_d == "ARG", .(common_legal_origin)]
dgd[year == 2000L & iso3_o == "ARG" & iso3_d == "CHL", .(common_legal_origin)]

dgd_legal <- dgd[, .(year, iso3_o, iso3_dynamic_o, iso3_d, iso3_dynamic_d, 
    common_legal_origin)][, `:=`(iso3_min = pmin(iso3_o, iso3_d), 
    iso3_max = pmax(iso3_o, iso3_d), iso3_dynamic_min = pmin(iso3_dynamic_o, 
        iso3_dynamic_d), iso3_dynamic_max = pmax(iso3_dynamic_o, 
        iso3_dynamic_d))][, .(common_legal_origin = max(common_legal_origin, 
    na.rm = TRUE)), keyby = .(iso3_min, iso3_max, iso3_dynamic_min, 
    iso3_dynamic_max)]

dgd_legal[iso3_min == "ARG" & iso3_max == "CHL", ]

dgd[ , common_legal_origin := NULL]

dgd <- dgd[, `:=`(iso3_min = pmin(iso3_o, iso3_d), iso3_max = pmax(iso3_o,
  iso3_d), iso3_dynamic_min = pmin(iso3_dynamic_o, iso3_dynamic_d),
  iso3_dynamic_max = pmax(iso3_dynamic_o, iso3_dynamic_d))][dgd_legal,
  on = .(iso3_min, iso3_max, iso3_dynamic_min, iso3_dynamic_max),
    nomatch = NULL, allow.cartesian = TRUE]

dgd[ , `:=`(iso3_min = NULL, iso3_max = NULL, iso3_dynamic_min = NULL, iso3_dynamic_max = NULL)]

setcolorder(dgd, "common_legal_origin", after = "common_colonizer")

# Write ----

setcolorder(dgd, c("region_id_o", "region_id_d"), after = "iso3_dynamic_d")
setorder(dgd, year, iso3_o, iso3_d)

dbWriteTable(con, "dgd", dgd, overwrite = TRUE)

dbDisconnect(con)
