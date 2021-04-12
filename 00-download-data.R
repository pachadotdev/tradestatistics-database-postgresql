# Open uncomtrade-datasets-arrow.Rproj before running this function

download <- function() {
  # messages ----------------------------------------------------------------

  msg <- c(
    "Apache License' Summary",
    "Permissions: Commercial use, modification, distribution, patent use, and private use.",
    "Limitations: Trademark use, liability, and warranty",
    "Conditions: License and copyright notice, and state changes.",
    "See https://github.com/pachamaltese/uncomtrade-datasets-arrow/blob/main/LICENSE."
  )

  message(paste(msg, collapse = "\n"))
  readline(prompt = "Press [ENTER] to continue if and only if you agree to the Apache License terms.")

  # scripts -----------------------------------------------------------------

  ask_for_token <<- 1
  ask_to_remove_old_files <<- 1

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

  if (nchar(old_file) > 0) {
    old_download_links <- as_tibble(fread(old_file)) %>%
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

  files_to_remove <- download_links %>%
    filter(local_file_date < server_file_date)

  years_to_update <- files_to_remove$year

  lapply(seq_along(years), data_downloading, dl = download_links)

  download_links <- download_links %>%
    select(year, url, new_file, local_file_date) %>%
    rename(file = new_file)

  download_links <- download_links %>%
    mutate(url = str_replace_all(url, "token=.*", "token=REPLACE_TOKEN"))

  fwrite(download_links, paste0(raw_dir, "/downloaded-files-", Sys.Date(), ".csv"))

  if (length(years_to_update) > 0) {
    fwrite(
      download_links %>% filter(year %in% years_to_update),
      paste0(raw_dir, "/updated-files-", Sys.Date(), ".csv")
    )
  }

  # save as arrow feather ---------------------------------------------------

  raw_zip <<- list.files(
    path = raw_dir_zip,
    pattern = "\\.zip",
    full.names = T
  ) %>%
    grep(paste(paste0("ps-", years_to_update), collapse = "|"), .,
         value = TRUE)

  # (remove) the old arrow files and create the arrow files again
  if (length(years_to_update) > 0) {
    lapply(seq_along(years_to_update), convert_to_arrow, yrs = years_to_update)
  }

  # also re-create any missing arrow dataset
  missing_years <- years[!file.exists(paste0(raw_dir_parquet, "/", years))]
  if (any(missing_years > 0)) {
    lapply(seq_along(missing_years), convert_to_arrow, yrs = missing_years)
  }
}

download()
