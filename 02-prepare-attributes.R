source("99-packages.R")

try(dir.create("attributes"))

saveRDS(
  tradestatistics::ots_commodities %>%
    mutate(
      section_code = case_when(
        section_code == "999" ~ "99",
        TRUE ~ section_code
      )
    ),
  "attributes/commodities.rds"
)

saveRDS(tradestatistics::ots_commodities_short, "attributes/commodities_short.rds")

saveRDS(
  tradestatistics::ots_sections %>%
    mutate(
      section_code = case_when(
        section_code == "999" ~ "99",
        TRUE ~ section_code
      )
    ),
  "attributes/sections.rds"
)

saveRDS(tradestatistics::ots_sections_colors, "attributes/sections_colors.rds")

saveRDS(
  tradestatistics::ots_countries,
  "attributes/countries.rds"
)
