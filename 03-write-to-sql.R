# Open uncomtrade-datasets-arrow.Rproj before running this function

source("99-packages.R")

con <- con_tradestatistics()

update_commodities(con)
update_countries(con)
update_sections(con)
update_yc(con)
update_yr(con)
update_yrc(con)
update_yrp(con)
update_yrpc(con)
update_rtas(con)
update_tariffs(con)
update_distances(con)
update_gdp_deflator(con)

RPostgres::dbDisconnect(con)
