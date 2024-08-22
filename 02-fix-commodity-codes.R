# any code not in the correspondence table will be converted to the closest
# match following the root in the HS/SITC tree

source("99-packages.R")

con <- dbConnect(
  Postgres(),
  host = "localhost",
  dbname = "tradestatistics",
  user = Sys.getenv("LOCAL_SQL_USR"),
  password = Sys.getenv("LOCAL_SQL_PWD")
)

classifications <- c(
  paste0("hs_rev", c(1992, 1996, 2002, 2007, 2012, 2017)),
  paste0("sitc_rev", c(1, 2, 3))
)

classifications2 <- c(
  paste0("hs", c(92, 96, "02", "07", 12, 17)),
  paste0("sitc", c(1, 2, 3))
)

tbls <- dbListTables(con)
tbls <- grep("_export|_import", tbls, value = T)
tbls <- grep(paste(classifications, collapse = "|"), tbls, value = T)
tbls <- sort(tbls)

# tbls <- tbls[3:length(tbls)]
# tbls <- tbls[23:length(tbls)]

# map(
#   tbls,
#   function(t) {
#     # t = tbls[1]
#     # ALTER INDEX public.hs_rev1992_tf_export_al_6_partner RENAME TO hs_rev1992_exports_partner;
#     # ALTER INDEX public.hs_rev1992_tf_export_al_6_reporter RENAME TO hs_rev1992_exports_reporter;
#     # ALTER INDEX public.hs_rev1992_tf_export_al_6_year RENAME TO hs_rev1992_exports_year;
#     # ALTER TABLE public.hs_rev1992_imports RENAME CONSTRAINT hs_rev1992_tf_import_al_6_commodity_code_fkey TO hs_rev1992_imports_commodity_code_fkey;
#     # ALTER TABLE public.hs_rev1992_imports RENAME CONSTRAINT hs_rev1992_tf_import_al_6_partner_iso_partner_code_fkey TO hs_rev1992_imports_partner_iso_partner_code_fkey;
#     # ALTER TABLE public.hs_rev1992_imports RENAME CONSTRAINT hs_rev1992_tf_import_al_6_qty_unit_code_fkey TO hs_rev1992_imports_qty_unit_code_fkey;
#     # ALTER TABLE public.hs_rev1992_imports RENAME CONSTRAINT hs_rev1992_tf_import_al_6_reporter_iso_reporter_code_fkey TO hs_rev1992_imports_reporter_iso_reporter_code_fkey;

#     is_hs <- grepl("hs_", t)

#     t2 <- str_replace(t, "ports", paste0("port_al_", ifelse(is_hs, 6, 5)))
#     t2 <- str_replace(t2, "_export_", "_tf_export_")
#     t2 <- str_replace(t2, "_import_", "_tf_import_")
#     t2 <- str_replace(t2, "_re_tf", "_tf_re")

#     dbSendQuery(
#       con,
#       glue(
#         "ALTER INDEX public.{t2}_partner RENAME TO {t}_partner"
#       )
#     )

#     dbSendQuery(
#       con,
#       glue(
#         "ALTER INDEX public.{t2}_reporter RENAME TO {t}_reporter"
#       )
#     )

#     dbSendQuery(
#       con,
#       glue(
#         "ALTER INDEX public.{t2}_year RENAME TO {t}_year"
#       )
#     )

#     dbSendQuery(
#       con,
#       glue(
#         "ALTER TABLE public.{t} RENAME CONSTRAINT {t2}_commodity_code_fkey TO {t}_commodity_code_fkey"
#       )
#     )

#     dbSendQuery(
#       con,
#       glue(
#         "ALTER TABLE public.{t} RENAME CONSTRAINT {t2}_partner_iso_partner_code_fkey TO {t}_partner_iso_partner_code_fkey"
#       )
#     )

#     dbSendQuery(
#       con,
#       glue(
#         "ALTER TABLE public.{t} RENAME CONSTRAINT {t2}_qty_unit_code_fkey TO {t}_qty_unit_code_fkey"
#       )
#     )

#     dbSendQuery(
#       con,
#       glue(
#         "ALTER TABLE public.{t} RENAME CONSTRAINT {t2}_reporter_iso_reporter_code_fkey TO {t}_reporter_iso_reporter_code_fkey"
#       )
#     )

#     return(TRUE)
#   }
# )

# extra codes for SITC ----

sitc_rds <- "attributes/sitc.rds"

if (!file.exists(sitc_rds)) {
sitc1_url <- "https://unstats.un.org/unsd/classifications/Econ/Download/In%20Text/sitc%20Rev1.xls"
sitc1_xls <- "attributes/sitc_rev1.xls"

if (!file.exists(sitc1_xls)) {
  download.file(sitc1_url, sitc1_xls)
}

sitc2_url <- "https://comtradeapi.un.org/files/v1/app/reference/S2.json"
sitc2_json <- "attributes/sitc_rev2.json"

if (!file.exists(sitc2_json)) {
  download.file(sitc2_url, sitc2_json)
}

sitc3_url <- "https://comtradeapi.un.org/files/v1/app/reference/S3.json"
sitc3_json <- "attributes/sitc_rev3.json"

if (!file.exists(sitc3_json)) {
  download.file(sitc3_url, sitc3_json)
}

sitc1 <- read_excel(sitc1_xls, sheet = 1) %>%
  clean_names() %>%
  rename(commodity_code = x0, classification = s1,
    description = food_and_live_animals)

sitc1 <- sitc1 %>%
  filter(nchar(commodity_code) == 5) %>%
  filter(commodity_code != "TOTAL") %>%
  distinct(commodity_code) %>%
  mutate(parent = str_sub(commodity_code, 1, 4))

sitc12 <- read_excel(sitc1_xls, sheet = 1) %>%
  clean_names() %>%
  rename(parent = x0, description = food_and_live_animals) %>%
  filter(nchar(parent) == 4) %>%
  mutate(description = str_trim(str_to_lower(iconv(description, "ASCII//TRANSLIT")))) %>%
  select(parent, classification = s1, description)

sitc1 <- sitc1 %>%
  inner_join(sitc12, by = "parent")

sitc2 <- fromJSON(sitc2_json)
# class(sitc2)
# names(sitc2)

sitc2 <- sitc2$results %>%
  as_tibble() %>%
  clean_names()

sitc2 <- sitc2 %>%
  select(commodity_code = id, parent) %>%
  filter(parent != "TOTAL") %>%
  filter(nchar(commodity_code) == 5) %>%
  filter(nchar(parent) == 5) %>%
  mutate(parent = str_remove(parent, "\\."))

sitc22 <- fromJSON(sitc2_json)$results %>%
  as_tibble() %>%
  clean_names() %>%
  select(commodity_code = id, description = text) %>%
  filter(nchar(commodity_code) == 4)

sitc22 <- sitc22 %>%
  mutate(description = str_sub(description, 8)) %>%
  distinct() %>%
  rename(parent = commodity_code)

sitc2 <- sitc2 %>%
  inner_join(sitc22, by = "parent") %>%
  mutate(description = str_trim(str_to_lower(iconv(description, "ASCII//TRANSLIT"))))

sitc2 <- sitc2 %>%
  mutate(classification = "S2")

sitc3 <- fromJSON(sitc3_json)

sitc3 <- sitc3$results %>%
  as_tibble() %>%
  clean_names()

sitc3 <- sitc3 %>%
  select(commodity_code = id, parent) %>%
  filter(parent != "TOTAL") %>%
  filter(nchar(commodity_code) == 5) %>%
  filter(nchar(parent) == 5) %>%
  mutate(parent = str_remove(parent, "\\."))

sitc32 <- fromJSON(sitc3_json)$results %>%
  as_tibble() %>%
  clean_names() %>%
  select(commodity_code = id, description = text) %>%
  filter(nchar(commodity_code) == 4)

sitc32 <- sitc32 %>%
  mutate(description = str_sub(description, 8)) %>%
  distinct() %>%
  rename(parent = commodity_code)

sitc3 <- sitc3 %>%
  inner_join(sitc32, by = "parent") %>%
  mutate(description = str_trim(str_to_lower(iconv(description, "ASCII//TRANSLIT"))))

sitc3 <- sitc3 %>%
  mutate(classification = "S3")

sitc <- bind_rows(sitc1, sitc2, sitc3)

rm(sitc1, sitc12, sitc2, sitc22, sitc3, sitc32)

saveRDS(sitc, sitc_rds)
} else {
sitc <- readRDS(sitc_rds)
}

# fix codes ----

map(
  tbls,
  function(t) {
    # t <- tbls[1]
    message(t)

    pos <- grep(gsub("_tf.*", "", t), classifications, value = F)

    codes <- as.character(classifications2[pos])
    codes <- tbl(con, "product_correlation") %>%
      collect() %>%
      select(matches(codes)) %>%
      distinct()

    colnames(codes) <- "commodity_code"

    codes <- codes %>%
      filter(nchar(commodity_code) == max(nchar(commodity_code)))

    years <- tbl(con, t) %>%
      distinct(year) %>%
      collect() %>%
      pull()

    current_codes <- tbl(con, t) %>%
      distinct(commodity_code) %>%
      collect()

    current_codes <- current_codes %>%
      anti_join(codes, by = "commodity_code")

    if (nrow(current_codes) == 0) {
      return(TRUE)
    }

    map(
      years,
      function(y) {
        # y = years[1]
        message(y)

        d <- tbl(con, t) %>%
          filter(year == y) %>%
          collect()

        d2 <- d %>%
          inner_join(codes, by = "commodity_code")

        d3 <- d %>%
          anti_join(codes, by = "commodity_code")

        if (grepl("sitc", t)) {
          d3 <- d3 %>%
            mutate(
              commodity_code = case_when(
                str_sub(commodity_code, 1, 4) == "9999" ~ "99999",
                TRUE ~ commodity_code
              )
            )

          classification2 <- switch(
            str_sub(t, 6, 9),
            rev1 = "S1",
            rev2 = "S2",
            rev3 = "S3"
          )

          sitc_desc <- d3 %>%
            distinct(commodity_code) %>%
            inner_join(
              sitc %>%
                filter(classification == classification2) %>%
                select(commodity_code, parent, description),
              by = "commodity_code"
            ) %>%
            select(commodity_code = parent, commodity = description) %>%
            distinct() %>%
            anti_join(
              tbl(con, str_replace(t, "tf.*", "commodities")) %>%
                select(commodity_code) %>%
                collect()
            )

            if (nrow(sitc_desc) > 0) {
              dbWriteTable(con, str_replace(t, "tf.*", "commodities"), sitc_desc, append = T, row.names = F)
            }

            d3 <- d3 %>%
              inner_join(
                sitc %>%
                  filter(classification == classification2) %>%
                  select(commodity_code, parent),
                by = "commodity_code"
              ) %>%
              mutate(commodity_code = parent) %>%
              select(-parent)
        } else {
          d3 <- d3 %>%
            mutate(
              commodity_code = case_when(
                str_sub(commodity_code, 1, 4) == "9999" ~ "999999",
                TRUE ~ paste0(str_sub(commodity_code, 1, 4), "00")
              )
            )
        }

        d3 <- d3 %>%
          group_by(year, reporter_iso, partner_iso, reporter_code, partner_code,
            commodity_code, qty_unit_code) %>%
          summarise(
            qty = sum(qty, na.rm = T),
            netweight_kg = sum(netweight_kg, na.rm = T),
            trade_value_usd = sum(trade_value_usd, na.rm = T)
          ) %>%
          ungroup()

        d <- d2 %>%
          bind_rows(d3) %>%
          arrange(year, reporter_iso, partner_iso, commodity_code,
            qty_unit_code)

        # remove observations for year y in the database
        dbSendQuery(con, glue("DELETE FROM {t} WHERE year = {y}"))

        # insert new observations for year y in the database
        dbWriteTable(con, t, d, append = T, row.names = F)

        return(TRUE)
      }
    )
  }
)

# sort commodities ----

tbls <- dbListTables(con)
tbls <- grep("_commodities$", tbls, value = T)
tbls <- grep(paste(classifications, collapse = "|"), tbls, value = T)
tbls <- sort(tbls)

# tbls <- tbls[2:length(tbls)]

map(
  tbls,
  function(t) {
    # t = tbls[1]
    message(t)

    d <- tbl(con, t) %>%
      collect() %>%
      arrange(commodity_code)

    # fix ",[a-z]" -> ", [a-z]"
    d <- d %>%
      mutate(commodity = str_replace_all(commodity, "\\,([a-z])", ", \\1"))
    
    d <- d %>%
      mutate(commodity = str_replace_all(commodity, "([a-z])\\,([0-9])", "\\1, \\2"))

    d <- d %>%
      mutate(commodity = str_replace_all(commodity, "([a-z])\\.([a-z])", "\\1. \\2"))

    # d %>%
    #   filter(commodity_code == "67422")

    # drop the foreign keys
    # hs_rev1992_imports_commody_code_fkey
    # hs_rev1992_exports_commody_code_fkey
    # hs_rev1992_re_imports_commody_code_fkey
    # hs_rev1992_re_exports_commody_code_fkey

    t2 <- str_replace(t, "_commodities", "")

    dbSendQuery(
      con,
      glue(
        "ALTER TABLE public.{t2}_exports DROP CONSTRAINT {t2}_exports_commodity_code_fkey"
      )
    )

    dbSendQuery(
      con,
      glue(
        "ALTER TABLE public.{t2}_imports DROP CONSTRAINT {t2}_imports_commodity_code_fkey"
      )
    )

    dbSendQuery(
      con,
      glue(
        "ALTER TABLE public.{t2}_re_exports DROP CONSTRAINT {t2}_re_exports_commodity_code_fkey"
      )
    )

    dbSendQuery(
      con,
      glue(
        "ALTER TABLE public.{t2}_re_imports DROP CONSTRAINT {t2}_re_imports_commodity_code_fkey"
      )
    )

    # overwrite the table
    dbWriteTable(con, t, d, overwrite = T, row.names = F)

    # re-add the constraint
    # ALTER TABLE public.hs_rev1992_exports ADD CONSTRAINT hs_rev1992_exports_commodity_code_fkey FOREIGN KEY (commodity_code) REFERENCES hs_rev1992_commodities(commodity_code)

    dbSendQuery(
      con,
      glue(
        "ALTER TABLE public.{t2}_commodities ADD CONSTRAINT {t2}_commodities_commodity_code_key UNIQUE (commodity_code)"
      )
    )

    dbSendQuery(
      con,
      glue(
        "ALTER TABLE public.{t2}_exports ADD CONSTRAINT {t2}_exports_commodity_code_fkey FOREIGN KEY (commodity_code) REFERENCES {t2}_commodities(commodity_code)"
      )
    )

    dbSendQuery(
      con,
      glue(
        "ALTER TABLE public.{t2}_imports ADD CONSTRAINT {t2}_imports_commodity_code_fkey FOREIGN KEY (commodity_code) REFERENCES {t2}_commodities(commodity_code)"
      )
    )

    dbSendQuery(
      con,
      glue(
        "ALTER TABLE public.{t2}_re_exports ADD CONSTRAINT {t2}_re_exports_commodity_code_fkey FOREIGN KEY (commodity_code) REFERENCES {t2}_commodities(commodity_code)"
      )
    )

    dbSendQuery(
      con,
      glue(
        "ALTER TABLE public.{t2}_re_imports ADD CONSTRAINT {t2}_re_imports_commodity_code_fkey FOREIGN KEY (commodity_code) REFERENCES {t2}_commodities(commodity_code)"
      )
    )

    return(TRUE)
  }
)

dbDisconnect(con)
