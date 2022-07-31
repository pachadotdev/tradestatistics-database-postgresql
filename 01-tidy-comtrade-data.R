source("99-packages.R")

map(
  2002:2020,
  function(y) {
    message(y)
    if (dir.exists(paste0("hs-rev2012-tidy/", y))) { return(TRUE) }
    tidy_flows(y) %>%
      group_by(year, reporter_iso) %>%
      write_dataset("hs-rev2012-tidy", hive_style = F)
  }
)
