# Open uncomtrade-datasets-arrow.Rproj before running this function

###########################################################################
# SOURCE THIS FILE, DON'T RUN BY SECTIONS, OR THE INTERACTIVE INPUT FAILS #
###########################################################################

# Apache License' Summary
# Permissions: Commercial use, modification, distribution, patent use, and private use.
# Limitations: Trademark use, liability, and warranty
# Conditions: License and copyright notice, and state changes.
# See https://github.com/pachadotdev/uncomtrade-datasets-arrow/blob/main/LICENSE.

# scripts -----------------------------------------------------------------

source("99-user-input.R")
source("99-input-based-parameters.R")
source("99-packages.R")
source("99-funs.R")
source("99-dirs-and-files.R")

# download data -----------------------------------------------------------

try(
  old_file <- max(
    list.files(raw_dir, pattern = "downloaded-files.*csv", full.names = T), na.rm = T)
)

if (isTRUE(nchar(old_file) > 0)) {
  old_download_links <- read_csv(old_file) %>%
    mutate(
      local_file_date = gsub(".*pub-", "", file),
      local_file_date = gsub("_fmt.*", "", local_file_date),
      local_file_date = as.Date(local_file_date, "%Y%m%d")
    ) %>%
    rename(old_file = file)
}

download_links <- tibble(
  year = years,
  url = paste0(
    "https://comtrade.un.org/api/get/bulk/C/A/",
    year,
    "/ALL/",
    classification2,
    "?token=",
    Sys.getenv("COMTRADE_TOKEN")
  ),
  file = NA
)

files <- fromJSON(sprintf(
  "https://comtrade.un.org/api/refs/da/bulk?freq=A&r=ALL&px=%s&token=%s",
  classification2,
  Sys.getenv("COMTRADE_TOKEN"))) %>%
  filter(ps %in% years) %>%
  arrange(ps)

if (exists("old_download_links")) {
  download_links <- download_links %>%
    mutate(
      file = paste0(raw_dir_zip, "/", files$name),
      server_file_date = gsub(".*pub-", "", file),
      server_file_date = gsub("_fmt.*", "", server_file_date),
      server_file_date = as.Date(server_file_date, "%Y%m%d")
    ) %>%
    left_join(old_download_links %>% select(-url), by = "year") %>%
    rename(new_file = file) %>%
    mutate(
      server_file_date = as.Date(
        ifelse(is.na(local_file_date), server_file_date + 1, server_file_date),
        origin = "1970-01-01"
      ),
      local_file_date = as.Date(
        ifelse(is.na(local_file_date), server_file_date - 1, local_file_date),
        origin = "1970-01-01"
      )
    )
} else {
  download_links <- download_links %>%
    mutate(
      file = paste0(raw_dir_zip, "/", files$name),

      server_file_date = gsub(".*pub-", "", file),
      server_file_date = gsub("_fmt.*", "", server_file_date),
      server_file_date = as.Date(server_file_date, "%Y%m%d"),

      # trick in case there are no old files
      old_file = NA,

      local_file_date = server_file_date,
      server_file_date = as.Date(server_file_date + 1, origin = "1970-01-01")
    ) %>%
    rename(new_file = file)
}

files_to_update <- download_links %>%
  filter(local_file_date < server_file_date)

files_to_update_2 <- download_links %>%
  mutate(file_exists = file.exists(new_file)) %>%
  filter(file_exists == F)

files_to_update <- files_to_update %>%
  bind_rows(files_to_update_2)

years_to_update <- files_to_update$year

lapply(seq_along(years), data_downloading, dl = download_links)

download_links <- download_links %>%
  select(year, url, new_file, local_file_date) %>%
  rename(file = new_file)

download_links <- download_links %>%
  mutate(url = str_replace_all(url, "token=.*", "token=REPLACE_TOKEN"))

if (length(years_to_update) > 0) {
  write_csv(download_links, paste0(raw_dir, "/downloaded-files-", Sys.Date(), ".csv"))

  write_csv(
    download_links %>% filter(year %in% years_to_update),
    paste0(raw_dir, "/updated-files-", Sys.Date(), ".csv")
  )
}

# save as arrow parquet ---------------------------------------------------

if (classification == "sitc") {
  aggregations <- 0:5
} else {
  aggregations <- 0:6
}

# re-create any missing/updated arrow dataset
raw_subdirs_parquet <- expand.grid(
  base_dir = raw_dir_parquet,
  aggregations = aggregations,
  flow = c("import", "export", "re-import", "re-export"),
  year = years
) %>%
  mutate(
    file = paste0(
      base_dir,
      "/aggregate_level=", aggregations, "/trade_flow=",
      flow, "/year=", year),
    exists = file.exists(file)
  )

update_years <- raw_subdirs_parquet %>%
  filter(exists == FALSE | year %in% years_to_update) %>%
  select(year) %>%
  distinct() %>%
  pull()

raw_zip <- list.files(
  path = raw_dir_zip,
  pattern = "\\.zip",
  full.names = T
) %>%
  grep(paste(paste0("ps-", update_years), collapse = "|"), .,
       value = TRUE)

if (any(update_years > 0)) {
  lapply(seq_along(update_years), convert_to_arrow, yrs = update_years)
}
