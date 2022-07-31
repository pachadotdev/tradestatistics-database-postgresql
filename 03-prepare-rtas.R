library(arrow)
library(dplyr)
library(tidyr)
library(desta)
library(tradestatistics)

d_rta <- in_force_ftas_dyads %>%
  left_join(
    ots_countries %>%
      select(countryx = country_iso, country1 = country_name_english)
  ) %>%
  left_join(
    ots_countries %>%
      select(countryy = country_iso, country2 = country_name_english)
  ) %>%
  mutate(
    country1 = iconv(tolower(country1), to = "ASCII//TRANSLIT", sub = ""),
    countryx = case_when(
      country1 == "antigua & barbuda" ~ "atg",
      country1 == "belgium" ~ "bel",
      country1 == "bosnia & herzegovina" ~ "bih",
      country1 == "british indian ocean territory" ~ "iot",
      country1 == "brunei" ~ "brn",
      country1 == "cayman islands" ~ "cym",
      country1 == "central african republic" ~ "caf",
      country1 == "congo - brazzaville" ~ "cog",
      country1 == "congo - kinshasa" ~ "cod",
      country1 == "cook islands" ~ "cok",
      country1 == "cote d'ivoire" ~ "civ",
      country1 == "czechia" ~ "cze",
      country1 == "cte divoire" ~ "civ",
      country1 == "dominican republic" ~ "dom",
      country1 == "falkland islands" ~ "flk",
      country1 == "french southern territories" ~ "atf",
      country1 == "hong kong sar china" ~ "hkg",
      country1 == "kazakhstan" ~ "kaz",
      country1 == "laos" ~ "lao",
      country1 == "libya" ~ "lby",
      country1 == "macao sar china" ~ "mac",
      country1 == "macedonia" ~ "mkd",
      country1 == "mayotte" ~ "myt",
      country1 == "marshall islands" ~ "mhl",
      country1 == "micronesia (federated states of)" ~ "fsm",
      country1 == "moldova" ~ "mda",
      country1 == "montenegro" ~ "mne",
      country1 == "myanmar (burma)" ~ "mmr",
      country1 == "netherlands antilles" ~ "ant",
      country1 == "pitcairn islands" ~ "pcn",
      country1 == "north korea" ~ "pkr",
      country1 == "russia" ~ "rus",
      country1 == "sao tome & principe" ~ "stp",
      country1 == "serbia" ~ "srb",
      country1 == "solomon islands" ~ "slb",
      country1 == "south georgia & south sandwich islands" ~ "sgs",
      country1 == "south korea" ~ "kor",
      country1 == "st. helena" ~ "shn",
      country1 == "st. kitts & nevis" ~ "kna",
      country1 == "st. pierre & miquelon" ~ "spm",
      country1 == "st. lucia" ~ "lca",
      country1 == "st. vincent & grenadines" ~ "vct",
      country1 == "syria" ~ "syr",
      country1 == "so tom & prncipe" ~ "stp",
      country1 == "tanzania" ~ "tza",
      country1 == "trinidad & tobago" ~ "tto",
      country1 == "turks & caicos islands" ~ "tca",
      country1 == "united states" ~ "usa",
      country1 == "vietnam" ~ "vnm",
      country1 == "wallis & futuna" ~ "wlf",
      TRUE ~ countryx
    )
  ) %>%
  mutate(
    country2 = iconv(tolower(country2), to = "ASCII//TRANSLIT", sub = ""),
    countryy = case_when(
      country2 == "antigua & barbuda" ~ "atg",
      country2 == "belgium" ~ "bel",
      country2 == "bosnia & herzegovina" ~ "bih",
      country2 == "british indian ocean territory" ~ "iot",
      country2 == "brunei" ~ "brn",
      country2 == "cayman islands" ~ "cym",
      country2 == "central african republic" ~ "caf",
      country2 == "congo - brazzaville" ~ "cog",
      country2 == "congo - kinshasa" ~ "cod",
      country2 == "cook islands" ~ "cok",
      country2 == "cote d'ivoire" ~ "civ",
      country2 == "czechia" ~ "cze",
      country2 == "cte divoire" ~ "civ",
      country2 == "dominican republic" ~ "dom",
      country2 == "falkland islands" ~ "flk",
      country2 == "french southern territories" ~ "atf",
      country2 == "hong kong sar china" ~ "hkg",
      country2 == "kazakhstan" ~ "kaz",
      country2 == "laos" ~ "lao",
      country2 == "libya" ~ "lby",
      country2 == "macao sar china" ~ "mac",
      country2 == "macedonia" ~ "mkd",
      country2 == "mayotte" ~ "myt",
      country2 == "marshall islands" ~ "mhl",
      country2 == "micronesia (federated states of)" ~ "fsm",
      country2 == "moldova" ~ "mda",
      country2 == "montenegro" ~ "mne",
      country2 == "myanmar (burma)" ~ "mmr",
      country2 == "netherlands antilles" ~ "ant",
      country2 == "pitcairn islands" ~ "pcn",
      country2 == "north korea" ~ "pkr",
      country2 == "russia" ~ "rus",
      country2 == "sao tome & principe" ~ "stp",
      country2 == "serbia" ~ "srb",
      country2 == "solomon islands" ~ "slb",
      country2 == "south georgia & south sandwich islands" ~ "sgs",
      country2 == "south korea" ~ "kor",
      country2 == "st. helena" ~ "shn",
      country2 == "st. kitts & nevis" ~ "kna",
      country2 == "st. pierre & miquelon" ~ "spm",
      country2 == "st. lucia" ~ "lca",
      country2 == "st. vincent & grenadines" ~ "vct",
      country2 == "syria" ~ "syr",
      country2 == "so tom & prncipe" ~ "stp",
      country2 == "tanzania" ~ "tza",
      country2 == "trinidad & tobago" ~ "tto",
      country2 == "turks & caicos islands" ~ "tca",
      country2 == "united states" ~ "usa",
      country2 == "vietnam" ~ "vnm",
      country2 == "wallis & futuna" ~ "wlf",
      TRUE ~ countryy
    )
  ) %>%
  select(-c(country1, country2)) %>%
  rename(country1 = countryx, country2 = countryy) %>%
  select(year, country1, country2, rta = in_force_fta) %>%
  drop_na() %>%
  filter(year >= 2002)

try(dir.create("rtas"))
saveRDS(d_rta, "rtas/rtas.rds")
