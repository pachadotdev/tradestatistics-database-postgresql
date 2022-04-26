# Open uncomtrade-datasets-arrow.Rproj before running this function

source("99-packages.R")

# save attributes tables

# try(dir.create("attributes"))
#
# saveRDS(
#   tradestatistics::ots_commodities %>%
#     mutate(
#       section_code = case_when(
#         section_code == "999" ~ "99",
#         TRUE ~ section_code
#       )
#     ),
#   "attributes/commodities.rds"
# )
# saveRDS(tradestatistics::ots_commodities_short, "attributes/commodities_short.rds")
# saveRDS(tradestatistics::ots_countries, "attributes/countries.rds")
# saveRDS(
#   tradestatistics::ots_sections %>%
#     mutate(
#       section_code = case_when(
#         section_code == "999" ~ "99",
#         TRUE ~ section_code
#       )
#     ),
#   "attributes/sections.rds"
# )
# saveRDS(tradestatistics::ots_sections_colors, "attributes/sections_colors.rds")

con <- con_tradestatistics()

update_commodities(con)
update_countries(con)
update_sections(con)
update_yc(con)
update_yr(con)
update_yrc(con)
update_yrp(con)
update_yrpc(con)

RPostgres::dbDisconnect(con)
