# Open uncomtrade-datasets-arrow.Rproj before running this function

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

saveRDS(readxl::read_xlsx("attributes/countries.xlsx"), "attributes/countries.rds")

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

# system("ln -s ../rtas-and-tariffs/mfn")
# system("ln -s ../rtas-and-tariffs/rtas")

try(dir.create("mfn-rtas"))

rtas <- open_dataset(
  "../rtas-and-tariffs/rtas/",
  partitioning = "year"
) %>%
  collect() %>%
  select(year, everything())

saveRDS(rtas, "mfn-rtas/rtas.rds")

tariffs <- open_dataset(
  "../rtas-and-tariffs/mfn/",
  partitioning = c("year", "reporter_iso")
) %>%
  collect() %>%
  select(year, reporter_iso, everything()) %>%
  mutate(sum_of_rates = round(sum_of_rates,2))

saveRDS(tariffs, "mfn-rtas/tariffs.rds")
