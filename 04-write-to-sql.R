source("99-packages.R")

y <- 2021:1980

con <- dbConnect(
  Postgres(),
  host = "localhost",
  dbname = "tradestatistics",
  user = Sys.getenv("LOCAL_SQL_USR"),
  password = Sys.getenv("LOCAL_SQL_PWD"),
  port = 5432
)

# Functions ----

commodity_conversion <- function(daux, y2) {
  if (y2 < 1995) {
    d2 <- tbl(con, "products_correlation") %>%
      select(commodity_orig = sitc2, commodity_code = hs12) %>%
      collect() %>%
      distinct()
  } else if (y2 >= 1995 && y2 < 2005) {
    d2 <- tbl(con, "products_correlation") %>%
      select(commodity_orig = hs92, commodity_code = hs12) %>%
      collect() %>%
      distinct()
  } else if (y2 >= 2005 && y2 < 2012) {
    d2 <- tbl(con, "products_correlation") %>%
      select(commodity_orig = hs02, commodity_code = hs12) %>%
      collect() %>%
      distinct()
  } else if (y2 >= 2012 && y2 < 2015) {
    d2 <- tbl(con, "products_correlation") %>%
      select(commodity_orig = hs07, commodity_code = hs12) %>%
      collect() %>%
      distinct()
  }

  d2 <- d2 %>%
    filter(nchar(commodity_orig) == max(nchar(commodity_orig)))

  d2 <- d2 %>%    
    filter(nchar(commodity_code) == 6L)

  daux <- daux %>%
    rename(commodity_orig = commodity_code) %>%
    left_join(d2, by = "commodity_orig") %>%
    rename(commodity_hs12 = commodity_code) %>%
    distinct(commodity_orig, .keep_all = T)

  # we need to convert codes that are not in the correlation table
  # example: 271011 -> 271000
  # aviation spirit -> oils petroleum, bituminous, distillates, except crude

  daux2 <- daux %>%
    filter(is.na(commodity_hs12))

  if (nrow(daux2) > 0) {
    daux2 <- daux2 %>%
      mutate(
        commodity_orig = case_when(
          str_sub(commodity_orig, 1, 4) == "9999" ~ "999999",
          TRUE ~ paste0(str_sub(commodity_orig, 1, 4), "00")
        )
      )

    daux2 <- daux2 %>%
      distinct(commodity_orig) %>%
      left_join(d2, by = "commodity_orig") %>%
      rename(commodity_hs12 = commodity_code) %>%
      distinct(commodity_orig, .keep_all = T)
    
    daux3 <- daux %>%
      filter(!is.na(commodity_hs12))

    daux <- daux2 %>%
      bind_rows(daux3) %>%
      distinct(commodity_orig, .keep_all = T)
  }

  daux
}

add_section_code <- function(d) {
  d %>%
    left_join(
      tbl(con, "commodities") %>%
        select(commodity_code, section_code) %>%
        collect() %>%
        distinct()
    )
}

set_trade_tables <- function(y2) {
  if (y2 < 1995) {
    tbl_exports <- "sitc_rev2_exports"
    tbl_imports <- "sitc_rev2_imports"
  } else if (y2 >= 1995 & y2 < 2005) {
    tbl_exports <- "hs_rev1992_exports"
    tbl_imports <- "hs_rev1992_imports"
  } else if (y2 >= 2005 & y2 < 2012) {
    tbl_exports <- "hs_rev2002_exports"
    tbl_imports <- "hs_rev2002_imports"
  } else if (y2 >= 2012 & y2 < 2015) {
    tbl_exports <- "hs_rev2007_exports"
    tbl_imports <- "hs_rev2007_imports"
  } else if (y2 >= 2015) {
    tbl_exports <- "hs_rev2012_exports"
    tbl_imports <- "hs_rev2012_imports"
  }

  assign("tbl_exports", tbl_exports, envir = parent.frame())
  assign("tbl_imports", tbl_imports, envir = parent.frame())
}

# Commodities ----

dbSendQuery(con, "DROP TABLE IF EXISTS public.commodities")

dbSendQuery(
  con,
  "CREATE TABLE public.commodities
      (
      commodity_code varchar(6) DEFAULT NULL,
      commodity_code_short varchar(4) DEFAULT NULL,
      commodity_fullname_english text DEFAULT NULL,
      section_code varchar(3) DEFAULT NULL
      )"
)

dbSendQuery(con, "DROP TABLE IF EXISTS public.commodities_short")

dbSendQuery(
  con,
  "CREATE TABLE public.commodities_short
      (
      commodity_code varchar(4) DEFAULT NULL,
      commodity_fullname_english text DEFAULT NULL
      )"
)

d <- readRDS("attributes/commodities.rds")

# unique(nchar(d$commodity_code))

d <- d %>%
  mutate(
    commodity_code_short = str_sub(commodity_code, 1, 4)
  ) %>%
  select(
    commodity_code, commodity_code_short, commodity_fullname_english,
    section_code
  )

dbWriteTable(con, "commodities", d, overwrite = T, row.names = F)

d2 <- readRDS("attributes/commodities_short.rds")

d %>%
  filter(nchar(commodity_code) == 3)

d2 <- d2 %>%
  bind_rows(
    d %>%
      filter(nchar(commodity_code) %in% 2:3) %>%
      select(commodity_code, commodity_fullname_english)
  ) %>%
  distinct() %>%
  arrange(commodity_code)

# unique(nchar(d$commodity_code))

# d2 <- tbl(con, "hs_rev2012_commodities") %>%
#   collect() %>%
#   mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>%
#   distinct(commodity_code)

# d %>%
#   anti_join(d2)

# d2 %>%
#   anti_join(d)

dbWriteTable(con, "commodities_short", d2, overwrite = T, row.names = F)

# Countries ----

dbSendQuery(con, "DROP TABLE IF EXISTS public.countries")

dbSendQuery(
  con,
  "CREATE TABLE public.countries
      (
      country_iso varchar(5) DEFAULT NULL,
      country_name_english text DEFAULT NULL,
      country_fullname_english text DEFAULT NULL,
      continent_id integer DEFAULT NULL,
      continent_name_english text DEFAULT NULL
      )"
)

dbSendQuery(con, "DROP TABLE IF EXISTS public.countries_colors")

dbSendQuery(
  con,
  "CREATE TABLE public.countries_colors
      (
      continent_id integer DEFAULT NULL,
      country_iso varchar(5) DEFAULT NULL,
      country_color char(7) DEFAULT NULL
      )"
)

d <- readRDS("attributes/countries.rds") %>%
  as_tibble()

dbWriteTable(con, "countries", d, overwrite = T, row.names = F)

d3 <- readRDS("attributes/countries_colors.rds")

saveRDS(d3, "attributes/countries_colors.rds")

dbWriteTable(con, "countries_colors", d3, overwrite = T, row.names = F)

# Sections ----

dbSendQuery(con, "DROP TABLE IF EXISTS public.sections")

dbSendQuery(
  con,
  "CREATE TABLE public.sections
      (
      section_code char(2) DEFAULT NULL,
      section_fullname_english text DEFAULT NULL
      )"
)

dbSendQuery(con, "DROP TABLE IF EXISTS public.sections_colors")

dbSendQuery(
  con,
  "CREATE TABLE public.sections_colors
      (
      section_code varchar(3) DEFAULT NULL,
      section_color char(7) DEFAULT NULL
      )"
)

dbWriteTable(con, "sections", readRDS("attributes/sections.rds"),
  overwrite = T, row.names = F
)

dbWriteTable(con, "sections_colors", readRDS("attributes/sections_colors.rds"),
  overwrite = T, row.names = F
)

# GDP deflator ----

dbSendQuery(con, "DROP TABLE IF EXISTS public.gdp_deflator")

dbSendQuery(
  con,
  "CREATE TABLE public.gdp_deflator
      (
      year_from integer DEFAULT NULL,
      year_to integer DEFAULT NULL,
      country_iso char(3) DEFAULT NULL,
      gdp_deflator decimal(5,4) DEFAULT NULL
      )"
)

d <- readRDS("gdp/gdp_deflator.rds")

d <- d %>%
  mutate(
    country_iso = ifelse(country_iso == "wld", "all", country_iso)
  )

# d2 <- tradestatistics::ots_gdp_deflator

unique(d$country_iso)

d2 <- tbl(con, "countries") %>%
  select(country_iso) %>%
  collect()

d <- d %>%
  inner_join(d2) %>%
  arrange(year_from, year_to, country_iso)

# d %>%
#   filter(country_iso == "all")

# d2 %>%
#   filter(country_iso == "all")

dbWriteTable(con, "gdp_deflator", d, overwrite = T, row.names = F)

# YRPC ----

dbSendQuery(con, "DROP TABLE IF EXISTS public.yrpc")

dbSendQuery(
  con,
  "CREATE TABLE public.yrpc
      (
      year integer NOT NULL,
      reporter_iso varchar(5) NOT NULL,
      partner_iso varchar(5) NOT NULL,
      section_code char(2) NOT NULL,
      commodity_code char(6) NOT NULL,
      trade_value_usd_imp decimal(16,2) DEFAULT NULL,
      trade_value_usd_exp decimal(16,2) DEFAULT NULL
      )"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_y ON public.yrpc (year)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_r ON public.yrpc (reporter_iso)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_p ON public.yrpc (partner_iso)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_c ON public.yrpc (commodity_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_s ON public.yrpc (section_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_yr ON public.yrpc (year, reporter_iso)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_yp ON public.yrpc (year, partner_iso)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_yc ON public.yrpc (year, commodity_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_ys ON public.yrpc (year, section_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_rp ON public.yrpc (reporter_iso, partner_iso)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_rpc ON public.yrpc (reporter_iso, partner_iso, commodity_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_yrp ON public.yrpc (year, reporter_iso, partner_iso)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_yrc ON public.yrpc (year, reporter_iso, commodity_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_yrs ON public.yrpc (year, reporter_iso, section_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_ypc ON public.yrpc (year, partner_iso, commodity_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrpc_yps ON public.yrpc (year, partner_iso, section_code)"
)

map(
  y,
  function(y2) {
    message(y2)

    tbl_exports <- NA
    tbl_imports <- NA
    set_trade_tables(y2)

    d <- tbl(con, tbl_exports) %>%
      select(
        year, reporter_iso, partner_iso, commodity_code,
        trade_value_usd
      ) %>%
      filter(year == y2) %>%
      filter(!reporter_iso %in% c("all", "wld")) %>%
      filter(!partner_iso %in% c("all", "wld")) %>%
      group_by(year, reporter_iso, partner_iso, commodity_code) %>%
      summarise(trade_value_usd_exp = sum(trade_value_usd, na.rm = T)) %>%
      ungroup() %>%
      full_join(
        tbl(con, tbl_imports) %>%
          select(
            year, reporter_iso, partner_iso, commodity_code,
            trade_value_usd
          ) %>%
          filter(year == y2) %>%
          filter(!reporter_iso %in% c("all", "wld")) %>%
          filter(!partner_iso %in% c("all", "wld")) %>%
          group_by(year, reporter_iso, partner_iso, commodity_code) %>%
          summarise(trade_value_usd_imp = sum(trade_value_usd, na.rm = T)) %>%
          ungroup(),
        by = c("year", "reporter_iso", "partner_iso", "commodity_code")
      ) %>%
      collect() %>%
      arrange(reporter_iso, partner_iso, commodity_code)

    d <- d %>%
      filter(trade_value_usd_imp > 0 & trade_value_usd_exp > 0) %>%
      filter(reporter_iso != "0-unspecified" & partner_iso != "0-unspecified")

    # daux <- d %>%
    #   group_by(reporter_iso, partner_iso, commodity_code) %>%
    #   count() %>%
    #   filter(n > 1)

    # stopifnot(nrow(daux) == 0)

    if (y2 < 2015) {
      daux <- d %>%
        distinct(commodity_code)

      daux <- commodity_conversion(daux, y2)

      d <- d %>%
        mutate(
          in_daux = as.integer(commodity_code %in% daux$commodity_orig)
        ) %>%
        mutate(
          commodity_code = case_when(
            str_sub(commodity_code, 1, 4) == "9999" ~ "999999",
            in_daux == 0L ~ paste0(str_sub(commodity_code, 1, 4), "00"),
            TRUE ~ commodity_code
          )
        ) %>%
        left_join(daux, by = c("commodity_code" = "commodity_orig")) %>%
        select(-commodity_code, -in_daux) %>%
        rename(commodity_code = commodity_hs12) %>%
        group_by(year, reporter_iso, partner_iso, commodity_code) %>%
        summarise(
          trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T),
          trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T)
        )
    }

    d <- add_section_code(d)

    d <- d %>%
      select(
        year, reporter_iso, partner_iso, section_code, commodity_code,
        trade_value_usd_imp, trade_value_usd_exp
      )

    dbWriteTable(con, "yrpc", d, append = T, overwrite = F, row.names = F)
  }
)

# YR ----

dbSendQuery(con, "DROP TABLE IF EXISTS public.yr")

dbSendQuery(
  con,
  "CREATE TABLE public.yr
      (
      year integer NOT NULL,
      reporter_iso varchar(5) NOT NULL,
      trade_value_usd_imp decimal(16,2) DEFAULT NULL,
      trade_value_usd_exp decimal(16,2) DEFAULT NULL
      )"
)

dbSendQuery(
  con,
  "CREATE INDEX yr_y ON public.yr (year)"
)

dbSendQuery(
  con,
  "CREATE INDEX yr_r ON public.yr (reporter_iso)"
)

dbSendQuery(
  con,
  "CREATE INDEX yr_yr ON public.yr (year, reporter_iso)"
)

map(
  y,
  function(y2) {
    message(y2)

    d <- tbl(con, "yrpc") %>%
      select(year, reporter_iso, trade_value_usd_exp, trade_value_usd_imp) %>%
      filter(year == y2) %>%
      group_by(year, reporter_iso) %>%
      summarise(
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T),
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T)
      ) %>%
      ungroup() %>%
      collect() %>%
      arrange(reporter_iso)

    # d %>%
    #   filter(reporter_iso == "ita")

    # d <- d %>%
    #   filter(trade_value_usd_imp > 0 & trade_value_usd_exp > 0) %>%
    #   filter(reporter_iso != "0-unspecified")

    dbWriteTable(con, "yr", d, append = T, overwrite = F, row.names = F)
  }
)

# YC ----

dbSendQuery(con, "DROP TABLE IF EXISTS public.yc")

dbSendQuery(
  con,
  "CREATE TABLE public.yc
      (
      year integer NOT NULL,
      section_code char(2) NOT NULL,
      commodity_code char(6) NOT NULL,
      trade_value_usd_imp decimal(16,2) DEFAULT NULL,
      trade_value_usd_exp decimal(16,2) DEFAULT NULL
      )"
)

dbSendQuery(
  con,
  "CREATE INDEX yc_y ON public.yc (year)"
)

dbSendQuery(
  con,
  "CREATE INDEX yc_s ON public.yc (section_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yc_c ON public.yc (commodity_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yc_ys ON public.yc (year, section_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yc_yc ON public.yc (year, commodity_code)"
)

map(
  y,
  function(y2) {
    message(y2)

    d <- tbl(con, "yrpc") %>%
      select(year, commodity_code, trade_value_usd_imp, trade_value_usd_exp) %>%
      filter(year == y2) %>%
      group_by(year, commodity_code) %>%
      summarise(
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T),
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T)
      ) %>%
      ungroup() %>%
      collect() %>%
      arrange(commodity_code)

    d <- add_section_code(d)

    d <- d %>%
      select(
        year, section_code, commodity_code, trade_value_usd_imp,
        trade_value_usd_exp
      )

    # d %>%
    #   filter(is.na(commodity_code))

    dbWriteTable(con, "yc", d, append = T, overwrite = F, row.names = F)
  }
)

# YRC ----

dbSendQuery(con, "DROP TABLE IF EXISTS public.yrc")

dbSendQuery(
  con,
  "CREATE TABLE public.yrc
      (
      year integer NOT NULL,
      reporter_iso varchar(5) NOT NULL,
      section_code char(2) NOT NULL,
      commodity_code char(6) NOT NULL,
      trade_value_usd_imp decimal(16,2) DEFAULT NULL,
      trade_value_usd_exp decimal(16,2) DEFAULT NULL
      )"
)

dbSendQuery(
  con,
  "CREATE INDEX yrc_y ON public.yrc (year)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrc_r ON public.yrc (reporter_iso)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrc_s ON public.yrc (section_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrc_c ON public.yrc (commodity_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrc_yr ON public.yrc (year, reporter_iso)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrc_ys ON public.yrc (year, section_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrc_yc ON public.yrc (year, commodity_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrc_rs ON public.yrc (reporter_iso, section_code)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrc_rc ON public.yrc (reporter_iso, commodity_code)"
)

map(
  y,
  function(y2) {
    message(y2)

    d <- tbl(con, "yrpc") %>%
      select(year, reporter_iso, commodity_code, trade_value_usd_imp,
        trade_value_usd_exp) %>%
      filter(year == y2) %>%
      group_by(year, reporter_iso, commodity_code) %>%
      summarise(
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T),
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T)
      ) %>%
      ungroup() %>%
      collect() %>%
      arrange(reporter_iso, commodity_code)

    d <- add_section_code(d)

    d <- d %>%
      select(
        year, reporter_iso, section_code, commodity_code,
        trade_value_usd_imp, trade_value_usd_exp
      )

    dbWriteTable(con, "yrc", d, append = T, overwrite = F, row.names = F)
  }
)

# YRP ----

dbSendQuery(con, "DROP TABLE IF EXISTS public.yrp")

dbSendQuery(
  con,
  "CREATE TABLE public.yrp
      (
      year integer NOT NULL,
      reporter_iso varchar(5) NOT NULL,
      partner_iso varchar(5) NOT NULL,
      trade_value_usd_imp decimal(16,2) DEFAULT NULL,
      trade_value_usd_exp decimal(16,2) DEFAULT NULL
      )"
)

dbSendQuery(
  con,
  "CREATE INDEX yrp_y ON public.yrp (year)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrp_r ON public.yrp (reporter_iso)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrp_p ON public.yrp (partner_iso)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrp_yr ON public.yrp (year, reporter_iso)"
)

dbSendQuery(
  con,
  "CREATE INDEX yrp_yp ON public.yrp (year, partner_iso)"
)

map(
  y,
  function(y2) {
    message(y2)

    d <- tbl(con, "yrpc") %>%
      select(year, reporter_iso, partner_iso, trade_value_usd_imp,
        trade_value_usd_exp) %>%
      filter(year == y2) %>%
      group_by(year, reporter_iso, partner_iso) %>%
      summarise(
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T),
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T)
      ) %>%
      ungroup() %>%
      collect() %>%
      arrange(reporter_iso, partner_iso)

    dbWriteTable(con, "yrp", d, append = T, overwrite = F, row.names = F)
  }
)

# Indexes and foreign keys ----

dbSendQuery(
  con,
  "ALTER TABLE public.commodities ADD CONSTRAINT commodities_commodities_key UNIQUE (commodity_code)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.countries ADD CONSTRAINT countries_countries_key UNIQUE (country_iso)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.yrpc ADD CONSTRAINT yrpc_commodities_fkey FOREIGN KEY (commodity_code) REFERENCES public.commodities(commodity_code)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.yrpc ADD CONSTRAINT yrpc_countries_r_fkey FOREIGN KEY (reporter_iso) REFERENCES public.countries(country_iso)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.yrpc ADD CONSTRAINT yrpc_countries_p_fkey FOREIGN KEY (partner_iso) REFERENCES public.countries(country_iso)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.yr ADD CONSTRAINT yr_countries_fkey FOREIGN KEY (reporter_iso) REFERENCES public.countries(country_iso)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.yc ADD CONSTRAINT yc_commodities_fkey FOREIGN KEY (commodity_code) REFERENCES public.commodities(commodity_code)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.yrc ADD CONSTRAINT yrc_countries_fkey FOREIGN KEY (reporter_iso) REFERENCES public.countries(country_iso)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.yrc ADD CONSTRAINT yrc_commodities_fkey FOREIGN KEY (commodity_code) REFERENCES public.commodities(commodity_code)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.yrp ADD CONSTRAINT yrp_countries_r_fkey FOREIGN KEY (reporter_iso) REFERENCES public.countries(country_iso)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.yrp ADD CONSTRAINT yrp_countries_p_fkey FOREIGN KEY (partner_iso) REFERENCES public.countries(country_iso)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.sections ADD CONSTRAINT sections_sections_key UNIQUE (section_code)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.sections_colors ADD CONSTRAINT section_colors_sections_fkey FOREIGN KEY (section_code) REFERENCES public.sections(section_code)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.countries_colors ADD CONSTRAINT countries_colors_countries_fkey FOREIGN KEY (country_iso) REFERENCES public.countries(country_iso)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.commodities ADD CONSTRAINT commodities_sections_fkey FOREIGN KEY (section_code) REFERENCES public.sections(section_code)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.gdp_deflator ADD CONSTRAINT gdp_deflator_countries_fk FOREIGN KEY (country_iso) REFERENCES public.countries(country_iso)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.commodities_short ADD CONSTRAINT commodities_short_commodities_key UNIQUE (commodity_code)"
)

dbSendQuery(
  con,
  "ALTER TABLE public.commodities ADD CONSTRAINT commodities_short_commodities_fkey FOREIGN KEY (commodity_code_short) REFERENCES public.commodities_short(commodity_code)"
)

dbDisconnect(con)
