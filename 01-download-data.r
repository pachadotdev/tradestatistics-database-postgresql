library(tidyverse)
library(janitor)
library(comtradr)

# add the env variable COMTRADE_PRIMARY to your environment file.
# usethis::edit_r_environ()
set_primary_comtrade_key(key = Sys.getenv("COMTRADE_UOFT_PRIMARY"))

dout <- "input"
dout2 <- "input/meta"
try(dir.create(dout, showWarnings = FALSE, recursive = TRUE))
try(dir.create(dout2, showWarnings = FALSE, recursive = TRUE))

country_codes_2 <- as_tibble(comtradr::country_codes)

# trade data ----

map(
  1988:2023,
  function(y) {
    # y = 1988
    message("Downloading data for year ", y)

    reporter_iso <- country_codes_2 %>%
      filter(entry_year <= y) %>%
      pull(iso_3)

    # drop any code with "_", two letters, or containing numbers
    reporter_iso <- reporter_iso[!grepl("_", reporter_iso)]
    reporter_iso <- str_trim(reporter_iso)
    reporter_iso <- reporter_iso[!grepl("^[A-Z]{2}$", reporter_iso)]
    reporter_iso <- reporter_iso[!grepl("[0-9]", reporter_iso)]
    reporter_iso <- sort(unique(reporter_iso))
    reporter_iso <- reporter_iso[nchar(reporter_iso) == 3]

    map(
      reporter_iso,
      function(iso) {
        message("Downloading data for ", iso)
        # iso = "USA"

        fout <- file.path(dout, paste0("comtrade_", iso, "_", y, ".rds"))

        if (file.exists(fout)) {
          message("File already exists, skipping download")
          return(TRUE)
        }

        d <- try(ct_get_bulk(
          type = "goods",
          frequency = "A",
          commodity_classification = "HS",
          reporter = iso,
          start_date = y,
          end_date = y,
          primary_token = get_primary_comtrade_key()
        ))

        if (inherits(d, "try-error")) {
          message("Error downloading data for ", iso)
          return(FALSE)
        }

        saveRDS(d, file = fout, compress = "xz")
      }
    )
  }
)

# meta ----

hs_url <- "https://unstats.un.org/unsd/classifications/Econ/download/In%20Text/HSCodeandDescription.xlsx"

hs_xlsx <- file.path(dout2, "HSCodeandDescription.xlsx")

if (!file.exists(hs_xlsx)) {
  message("Downloading HS code file")
  download.file(hs_url, hs_xlsx)
} else {
  message("HS code file already exists, skipping download")
}

sitc_url <- "https://unstats.un.org/unsd/classifications/Econ/Download/In%20Text/SITCCodeandDescription.xlsx"

sitc_xlsx <- file.path(dout2, "SITCCodeandDescription.xlsx")

if (!file.exists(sitc_xlsx)) {
  message("Downloading SITC code file")
  download.file(sitc_url, sitc_xlsx)
} else {
  message("SITC code file already exists, skipping download")
}

bec_url <- "https://unstats.un.org/unsd/classifications/Econ/Download/In%20Text/BECCodeandDescription.xlsx"

bec_xlsx <- file.path(dout2, "BECCodeandDescription.xlsx")

if (!file.exists(bec_xlsx)) {
  message("Downloading BEC code file")
  download.file(bec_url, bec_xlsx)
} else {
  message("BEC code file already exists, skipping download")
}

correlation_url <- "https://unstats.un.org/unsd/classifications/Econ/tables/HS-SITC-BEC%20Correlations_2022.xlsx"

correlation_xlsx <- file.path(dout2, "HS-SITC-BEC_Correlations_2022.xlsx")

if (!file.exists(correlation_xlsx)) {
  message("Downloading correlation file")
  download.file(correlation_url, correlation_xlsx)
} else {
  message("Correlation file already exists, skipping download")
}
