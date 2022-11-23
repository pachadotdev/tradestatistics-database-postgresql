# Open inflation-data.Rproj before running this script.

# This program creates and updates the discount_rates table with the latest
# available information.
# Source information:
# https://data.worldbank.org/indicator/NY.GDP.DEFL.KD.ZG

# Package dependencies:
library(wbstats)
library(dplyr)
library(tidyr)
library(matrixStats)
library(readr)
library(jsonlite)

try(dir.create("gdp"))

gdp_current <- wbstats::wb_data(indicator = "NY.GDP.MKTP.CD")
gdp_percap_current <- wbstats::wb_data(indicator = "NY.GDP.PCAP.CD")

gdp_constant_2010_usd <- wbstats::wb_data(indicator = "NY.GDP.MKTP.KD")
gdp_deflator_annual_pct <- wbstats::wb_data(indicator = "NY.GDP.DEFL.KD.ZG")

# GDP ----

d <- gdp_current %>%
  select(iso3c, date, gdp = NY.GDP.MKTP.CD) %>%
  drop_na() %>%
  rename(
    country_iso = iso3c,
    year = date
  ) %>%
  mutate(
    country_iso = tolower(country_iso),
    country_iso = ifelse(country_iso == "rou", "rom", country_iso)
  )

d2 <- gdp_percap_current %>%
  select(iso3c, date, gdp_percap = NY.GDP.PCAP.CD) %>%
  drop_na() %>%
  rename(
    country_iso = iso3c,
    year = date
  ) %>%
  mutate(
    country_iso = tolower(country_iso),
    country_iso = ifelse(country_iso == "rou", "rom", country_iso)
  )

d <- d %>%
  left_join(d2, by = c("country_iso", "year"))

d <- d %>%
  filter(year >= 2002) %>%
  select(year, country_iso, everything())

saveRDS(d, "gdp/gdp.rds")

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
    country_iso = tolower(country_iso),
    country_iso = ifelse(country_iso == "rou", "rom", country_iso)
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
        country_iso = tolower(country_iso),
        country_iso = ifelse(country_iso == "rou", "rom", country_iso)
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
  filter(from >= 2002, to > from) %>%
  select(year_from = from, year_to = to, country_iso, everything())

saveRDS(d, "gdp/gdp_deflator.rds")
