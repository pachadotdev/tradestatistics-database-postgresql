source("99-packages.R")

try(dir.create("attributes"))

saveRDS(
  read_excel("attributes/commodities.xlsx"),
  "attributes/commodities.rds"
)

saveRDS(
  read_excel("attributes/commodities_short.xlsx"),
  "attributes/commodities_short.rds"
)

saveRDS(
  read_excel("attributes/sections.xlsx"),
  "attributes/sections.rds"
)

saveRDS(
  read_excel("attributes/sections_colors.xlsx"),
  "attributes/sections_colors.rds"
)

saveRDS(
  read_excel("attributes/countries.xlsx"),
  "attributes/countries.rds"
)
