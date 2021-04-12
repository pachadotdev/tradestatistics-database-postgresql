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
    try(file.remove(dl$old_file[t]))
  }
  if (!file.exists(dl$new_file[t])) {
    message(paste("Downloading", dl$new_file[t]))
    if (dl$local_file_date[t] < dl$server_file_date[t]) {
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
      message(paste(
        "Existing data is not older than server data. Skipping",
        dl$new_file[t]
      ))
    }
  } else {
    message(paste(dl$new_file[t], "exists. Skiping."))
  }
}

convert_to_arrow <- function(t, yrs = years_to_update) {
  messageline(yrs[t])

  if (remove_old_files == 1) {
    try(
      unlink(paste0(raw_dir_parquet, "/", yrs[t]), recursive = T)
    )
  }

  zip <- grep(paste0(yrs[t], "_freq-A"), raw_zip, value = T)

  csv <- zip %>%
    str_replace("/zip/", "/parquet/") %>%
    str_replace("zip$", "csv")

  extract(zip, raw_dir_parquet)

  d <- fread(
    input = csv,
    colClasses = list(
      character = "Commodity Code",
      numeric = c("Trade Value (US$)", "Qty", "Netweight (kg)")
    ))

  d <- d %>%
    mutate(
      `Reporter ISO` = case_when(
        `Reporter ISO` %in% c(NA, "", " ") ~ "0-unspecified",
        TRUE ~ `Reporter ISO`
      ),
      `Partner ISO` = case_when(
        `Partner ISO` %in% c(NA, "", " ") ~ "0-unspecified",
        TRUE ~ `Partner ISO`
      )
    )

  d %>%
    group_by(Year, `Reporter ISO`) %>%
    write_dataset(raw_dir_parquet, hive_style = F, max_partitions = 1024L)

  rm(d); file_remove(csv); gc()
}
