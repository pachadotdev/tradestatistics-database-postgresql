# Open inflation-data.Rproj before running this script.

# This program creates and updates the discount_rates table with the latest
# available information.
# Source information:
# https://data.worldbank.org/indicator/NY.GDP.DEFL.KD.ZG

try(dir.create("gdp"))

gdp_constant_2010_usd <- wbstats::wb_data(indicator = "NY.GDP.MKTP.KD")
gdp_deflator_annual_pct <- wbstats::wb_data(indicator = "NY.GDP.DEFL.KD.ZG")

# GDP deflator ----

d <- gdp_deflator_annual_pct %>%
  select(iso3c, date, deflator = NY.GDP.DEFL.KD.ZG) %>%
  drop_na() %>%
  mutate(gdp_deflator = 1 + deflator / 100) %>%
  rename(
    country_iso = iso3c,
    to = date
  ) %>%
  mutate(
    country_iso = tolower(country_iso)
  ) %>%
  mutate(
    to = as.integer(to),
    from = to - 1
  ) %>%
  select(country_iso, from, to, gdp_deflator)

d2 <- d %>%
  inner_join(
    gdp_constant_2010_usd %>%
      select(country_iso = iso3c, to = date, gdp = NY.GDP.MKTP.KD) %>%
      drop_na() %>%
      mutate(
        country_iso = tolower(country_iso)
      )
  ) %>%
  group_by(from, to) %>%
  summarise(
    gdp_deflator = weightedMedian(gdp_deflator, gdp)
  ) %>%
  mutate(country_iso = "wld")

d <- d %>%
  bind_rows(d2) %>%
  arrange(country_iso, from, to)

d <- d %>%
  filter(to > from) %>%
  select(year_from = from, year_to = to, country_iso, everything())

d <- d %>%
  filter(year_from >= 1980 & year_from <= 2020) %>%
  filter(year_to >= 1981 & year_to <= 2021)

saveRDS(d, "gdp/gdp_deflator.rds")
