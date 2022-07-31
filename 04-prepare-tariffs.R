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

# PREFERENTIAL RATES ----

fcsv <- list.files("prf", pattern = "csv$", full.names = T)
fcsv <- grep(paste(2002:2018, collapse = "|"), fcsv, value = T)
fcsv2 <- list.files("mfn", pattern = "csv$", full.names = T)
fcsv2 <- grep(paste(2002:2018, collapse = "|"), fcsv2, value = T)

prf <- map2(
  fcsv,
  fcsv2,
  function(x,y) {
    if (paste0("tariffs/", y)) { return(TRUE) }

    message(paste0(x, "\n", y))

    prf <- read_csv(x,
                    col_types = cols(
                      reporter = col_character(),
                      partner = col_character(), code = col_character(),
                      hs = col_character(), prf = col_double(),
                      year = col_integer()))

    z <- unique(prf$year)

    prf <- prf %>%
      mutate(hs = if_else(nchar(hs) == 5, paste0("0", hs), hs)) %>%
      # filter(prf > 0) %>%
      select(-year) %>%
      group_by(reporter) %>%
      nest()

    mfn <- read_csv(y,
                    col_types = cols(
                      reporter = col_character(),
                      hs = col_character(), mfn = col_number(),
                      year = col_number())) %>%
      mutate(hs = if_else(nchar(hs) == 5, paste0("0", hs), hs)) %>%
      rename(reporter_iso = reporter, commodity_code = hs) %>%
      select(-year) %>%
      mutate_if(is.character, str_to_lower) %>%
      inner_join(
        ots_countries %>%
          select(reporter_iso = country_iso) %>%
          as_tibble()
      ) %>%
      inner_join(
        ots_commodities %>%
          select(commodity_code) %>%
          as_tibble()
      )

    prf <- map_df(
      prf %>% select(reporter) %>% arrange(reporter) %>% pull(),
      function(r) {
        message(r)

        prf %>%
          filter(reporter == r) %>%
          unnest(data) %>%
          ungroup() %>%
          rename(reporter_iso = reporter, partner_iso = partner,
                 commodity_code = hs) %>%
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
          inner_join(
            ots_commodities %>%
              select(commodity_code) %>%
              as_tibble()
          ) %>%
          group_by(reporter_iso, partner_iso, commodity_code) %>%
          summarise(prf = mean(prf, na.rm = TRUE)) %>%
          ungroup()
      }
    )

    gc()

    # unique(nchar(prf$hs))
    # unique(nchar(mfn$hs))

    d <- crossing(
      reporter_iso = mfn %>%
        select(reporter_iso) %>%
        distinct() %>%
        bind_rows(
          prf %>%
            select(reporter_iso) %>%
            distinct()
        ) %>%
        distinct() %>%
        pull(),
      partner_iso = prf %>%
        select(partner_iso) %>%
        distinct() %>%
        pull(),
      commodity_code = mfn %>%
        select(commodity_code) %>%
        distinct() %>%
        bind_rows(
          prf %>%
            select(commodity_code) %>%
            distinct()
        ) %>%
        distinct() %>%
        pull()
    )

    d <- d %>%
      left_join(
        prf %>% select(reporter_iso, partner_iso, commodity_code, prf)
      ) %>%
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
          mutate(tariff = pmin(prf, mfn, na.rm = TRUE)) %>%
          select(-prf, -mfn) %>%
          # filter(tariff > 0) %>%
          mutate(year = z) %>%
          select(year, everything()) %>%
          group_by(year, reporter_iso) %>%
          write_dataset("tariffs", partitioning = c("year","reporter_iso"),
                        hive_style = F)

        return(TRUE)
      }
    )

    rm(d)
    gc()

    return(TRUE)
  }
)
