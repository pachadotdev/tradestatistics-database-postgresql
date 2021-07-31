messageline <- function(txt = NULL, width = 80) {
  if(is.null(txt)) {
    message(rep("-", width))
  } else {
    message(txt, " ", rep("-", width - nchar(txt) - 1))
  }
}

extract <- function(x, y) {
  if (file.exists(str_replace(x, "zip", "csv"))) {
    messageline()
    message(paste(x, "already unzipped. Skipping."))
  } else {
    messageline()
    message(paste("Unzipping", x))
    system(sprintf("7z e -aos %s -oc:%s", x, y))
  }
}

file_remove <- function(x) {
  try(file.remove(x))
}

data_downloading <- function(t,dl) {
  if (remove_old_files == 1 &
      (dl$local_file_date[t] < dl$server_file_date[t]) &
      !is.na(dl$old_file[t])) {
    file_remove(dl$old_file[t])
    file_remove(list.files(raw_dir_parquet, pattern = as,character(dl$year[t]),
                           recursive = T, include.dirs = T))
  }
  if (!file.exists(dl$new_file[t])) {
    message(paste("Downloading", dl$new_file[t]))
    Sys.sleep(sample(seq(5, 10, by = 1), 1))
    try(
      download.file(dl$url[t],
                    dl$new_file[t],
                    method = "wget",
                    quiet = T,
                    extra = "--no-check-certificate"
      )
    )

    if (file.size(dl$new_file[t]) == 0) {
      fs <- 1
    } else {
      fs <- 0
    }

    while (fs > 0) {
      try(
        download.file(dl$url[t],
                      dl$new_file[t],
                      method = "wget",
                      quiet = T,
                      extra = "--no-check-certificate"
        )
      )

      if (file.size(dl$new_file[t]) == 0) {
        fs <- fs + 1
      } else {
        fs <- 0
      }
    }
  } else {
    message(paste(dl$new_file[t], "exists. Skiping."))
  }
}

unspecified <- function(x) {
  case_when(
    x %in% c(NA, "") ~ "0-unspecified",
    TRUE ~ x
  )
}

convert_to_arrow <- function(t, yrs) {
  messageline(yrs[t])

  try(unlink(grep(paste0("year=",yrs[t]), raw_subdirs_parquet$file, value = T), recursive = T))

  zip <- grep(paste0(yrs[t], "_freq-A"), raw_zip, value = T)

  csv <- zip %>%
    str_replace("/zip/", "/parquet/") %>%
    str_replace("zip$", "csv")

  extract(zip, raw_dir_parquet)

  d <- read_csv(
    csv,
    col_types = cols(
      Classification = col_character(),
      Year = col_integer(),
      Period = col_integer(),
      `Period Desc.` = col_integer(),
      `Aggregate Level` = col_integer(),
      `Is Leaf Code` = col_integer(),
      `Trade Flow Code` = col_integer(),
      `Trade Flow` = col_character(),
      `Reporter Code` = col_integer(),
      Reporter = col_character(),
      `Reporter ISO` = col_character(),
      `Partner Code` = col_integer(),
      Partner = col_character(),
      `Partner ISO` = col_character(),
      `Commodity Code` = col_character(),
      Commodity = col_character(),
      `Qty Unit Code` = col_integer(),
      `Qty Unit` = col_character(),
      Qty = col_double(),
      `Netweight (kg)` = col_double(),
      `Trade Value (US$)` = col_double(),
      Flag = col_integer())
  )

  d <- d %>%
    clean_names() %>%
    rename(trade_value_usd = trade_value_us) %>%
    mutate_if(is.character, function(x) { str_to_lower(str_squish(x)) }) %>%
    mutate(
      reporter_iso = unspecified(reporter_iso),
      partner_iso = unspecified(partner_iso),
      trade_flow = unspecified(trade_flow)
    )

  al <- sort(unique(d$aggregate_level))

  tf <- sort(unique(d$trade_flow))

  map(
    al,
    function(x) {
      d2 <- d %>%
        filter(aggregate_level == x)

      gc()

      if (nrow(d2) > 0) {
        map(
          tf,
          function(x) {
            d2 %>%
              filter(trade_flow == x) %>%
              group_by(aggregate_level, trade_flow, year, reporter_iso) %>%
              write_dataset(raw_dir_parquet, hive_style = T)

            gc()
          }
        )
      }

      rm(d2); gc()
    }
  )

  file_remove(csv); rm(d); gc()
}
