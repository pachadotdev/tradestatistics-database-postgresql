library(purrr)
library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(arrow)
library(tradestatistics)

# see https://en.wikipedia.org/wiki/Country_codes:_A etc
# https://en.wikipedia.org/wiki/Country_codes_of_Serbia
fix_iso_codes <- function(val) {
  case_when(
    val == "rom" ~ "rou", # Romania
    val == "tmp" ~ "tls", # East Timor
    val == "ser" ~ "srb", # Serbia
    val == "sud" ~ "sdn", # Sudan
    val == "zar" ~ "cod", # Congo (Democratic Republic of the)
    val == "twn" ~ "e-490", # Other Asia, not elsewhere specified
    val == "eun" ~ "e-492", # European Union, not elsewhere specified
    TRUE ~ val
  )
}

eu_years <- read_csv("eu_members/EU-years.csv") %>%
  select(reporter_iso = iso3, year) %>%
  mutate(reporter_iso = str_to_lower(reporter_iso), eu_member = 1L)

eu_years_2 <- expand_grid(
  reporter_iso = str_to_lower(eu_years$reporter_iso),
  year = min(eu_years$year):2020
) %>%
  arrange(reporter_iso, year) %>%
  left_join(eu_years) %>%
  group_by(reporter_iso) %>%
  fill(eu_member, .direction = "down") %>%
  mutate(eu_member = if_else(is.na(eu_member), 0L, eu_member)) %>%
  mutate(
    eu_member = case_when(
      reporter_iso == "gbr" & year >= 2020 ~ 0L,
      TRUE ~ eu_member
    )
  ) %>%
  filter(year >= 2002 & year <= 2020)

rm(eu_years)

fcsv <- list.files("prf", pattern = "csv$", full.names = T)
fcsv <- grep(paste(2002:2018, collapse = "|"), fcsv, value = T)
fcsv2 <- list.files("mfn", pattern = "csv$", full.names = T)
fcsv2 <- grep(paste(2002:2018, collapse = "|"), fcsv2, value = T)

prf <- map2(
  fcsv,
  fcsv2,
  function(x,y) {
    z <- as.integer(gsub(".*-|\\.csv", "", x))
    if (file.exists(paste0("tariffs/", z))) { return(TRUE) }

    message(paste0(x, "\n", y))

    sections <- ots_commodities %>%
      distinct(section_code) %>%
      filter(section_code != "99") %>%
      arrange(section_code) %>%
      pull()

    map(
      sections,
      function(s) {
        message(s)
        prf <- read_csv(x,
                        col_types = cols(
                          reporter = col_character(),
                          partner = col_character(), code = col_character(),
                          hs = col_character(), prf = col_double(),
                          year = col_integer())) %>%
          mutate(hs = if_else(nchar(hs) == 5, paste0("0", hs), hs)) %>%
          # filter(prf > 0) %>%
          rename(reporter_iso = reporter, partner_iso = partner,
                 commodity_code = hs) %>%
          inner_join(
            ots_commodities %>%
              select(commodity_code, section_code) %>%
              filter(section_code == s) %>%
              as_tibble()
          ) %>%
          mutate_if(is.character, str_to_lower) %>%
          mutate(
            reporter_iso = fix_iso_codes(reporter_iso),
            partner_iso = fix_iso_codes(partner_iso)
          ) %>%
          inner_join(
            ots_countries %>%
              select(reporter_iso = country_iso) %>%
              as_tibble()
          ) %>%
          inner_join(
            ots_countries %>%
              select(partner_iso = country_iso) %>%
              as_tibble()
          ) %>%
          select(-year)

        prf <- prf %>%
          group_by(reporter_iso, partner_iso, section_code, commodity_code) %>%
          summarise(prf = min(prf, na.rm = T))

        prf_1 <- prf %>% filter(reporter_iso != "e-492")
        prf_2 <- prf %>% anti_join(prf_1)

        prf_2 <- eu_years_2 %>%
          filter(year == z, eu_member == 1L) %>%
          rename(reporter_iso.y = reporter_iso) %>%
          mutate(reporter_iso = "e-492") %>%
          full_join(prf_2) %>%
          select(reporter_iso = reporter_iso.y, partner_iso, section_code, commodity_code, prf) %>%
          filter(!is.na(prf))

        prf <- prf_1 %>% bind_rows(prf_2)

        prf_1 <- prf %>% filter(partner_iso != "e-492")
        prf_2 <- prf %>% anti_join(prf_1)

        prf_2 <- eu_years_2 %>%
          filter(year == z, eu_member == 1L) %>%
          rename(partner_iso.y = reporter_iso) %>%
          mutate(partner_iso = "e-492") %>%
          full_join(prf_2) %>%
          # mutate(reporter_iso.y = if_else(is.na(reporter_iso.y), reporter_iso, reporter_iso.y)) %>%
          select(reporter_iso, partner_iso = partner_iso.y, section_code, commodity_code, prf) %>%
          filter(!is.na(prf))

        prf <- prf_1 %>% bind_rows(prf_2)

        rm(prf_1, prf_2)

        prf <- prf %>%
          group_by(reporter_iso, partner_iso, commodity_code) %>%
          summarise(prf = min(prf, na.rm = T))

        # stopifnot(all.equal(
        #   prf %>% nrow(),
        #   prf %>%
        #     group_by(reporter_iso, partner_iso, commodity_code) %>%
        #     distinct() %>%
        #     nrow()
        # ))

        mfn <- read_csv(y,
                        col_types = cols(
                          reporter = col_character(),
                          hs = col_character(), mfn = col_number(),
                          year = col_number())) %>%
          mutate(hs = if_else(nchar(hs) == 5, paste0("0", hs), hs)) %>%
          rename(reporter_iso = reporter, commodity_code = hs) %>%
          select(-year) %>%
          inner_join(
            ots_commodities %>%
              select(commodity_code, section_code) %>%
              filter(section_code == s) %>%
              as_tibble()
          ) %>%
          mutate_if(is.character, str_to_lower) %>%
          mutate(
            reporter_iso = fix_iso_codes(reporter_iso)
          ) %>%
          inner_join(
            ots_countries %>%
              select(reporter_iso = country_iso) %>%
              as_tibble()
          )

        mfn_1 <- mfn %>% filter(reporter_iso != "e-492")
        mfn_2 <- mfn %>% anti_join(mfn_1)

        mfn_2 <- eu_years_2 %>%
          filter(year == z, eu_member == 1L) %>%
          rename(reporter_iso.y = reporter_iso) %>%
          mutate(reporter_iso = "e-492") %>%
          full_join(mfn_2) %>%
          # mutate(reporter_iso.y = if_else(is.na(reporter_iso.y), reporter_iso, reporter_iso.y)) %>%
          select(reporter_iso = reporter_iso.y, commodity_code, mfn)

        mfn <- mfn_1 %>%
          bind_rows(mfn_2) %>%
          group_by(reporter_iso, commodity_code) %>%
          summarise(mfn = min(mfn, na.rm = T)) %>%
          filter(!is.na(mfn))

        rm(mfn_1, mfn_2)

        # stopifnot(all.equal(
        #   mfn %>% nrow(),
        #   mfn %>%
        #     group_by(reporter_iso, commodity_code) %>%
        #     distinct() %>%
        #     nrow()
        # ))

        d <- crossing(
          reporter_iso = mfn %>%
            ungroup() %>%
            select(reporter_iso) %>%
            distinct() %>%
            bind_rows(
              prf %>%
                ungroup() %>%
                select(reporter_iso) %>%
                distinct()
            ) %>%
            distinct() %>%
            pull(),
          partner_iso = prf %>%
            ungroup() %>%
            select(partner_iso) %>%
            distinct() %>%
            pull(),
          commodity_code = mfn %>%
            ungroup() %>%
            select(commodity_code) %>%
            distinct() %>%
            bind_rows(
              prf %>%
                ungroup() %>%
                select(commodity_code) %>%
                distinct()
            ) %>%
            distinct() %>%
            pull()
        )

        d <- d %>%
          left_join(
            prf %>%
              select(reporter_iso, partner_iso, commodity_code, prf)
          )

        d <- d %>%
          left_join(
            mfn %>% select(reporter_iso, commodity_code, mfn)
          )

        rm(prf, mfn)
        gc()

        d <- d %>%
          group_by(reporter_iso) %>%
          nest()

        map(
          d %>% select(reporter_iso) %>% arrange(reporter_iso) %>% pull(),
          function(r) {
            d %>%
              filter(reporter_iso == r) %>%
              unnest(data) %>%
              mutate(
                tariff = pmin(prf, mfn, na.rm = TRUE),
                source = case_when(
                  tariff == prf ~ "prf",
                  tariff == mfn ~ "mfn",
                  is.na(tariff) ~ NA_character_
                )
              ) %>%
              select(-prf, -mfn) %>%
              filter(is.finite(tariff)) %>%
              mutate(year = z) %>%
              select(year, everything()) %>%
              mutate(section_code = s) %>%
              group_by(year, reporter_iso, section_code) %>%
              write_dataset("tariffs",
                            partitioning = c("year","reporter_iso","section_code"),
                            hive_style = F)

            return(TRUE)
          }
        )

        return(TRUE)
      }
    )

    gc()
    return(TRUE)
  }
)
