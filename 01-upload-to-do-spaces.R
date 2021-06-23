library(arrow)
library(dplyr)

space <- S3FileSystem$create(
  access_key = Sys.getenv('DO_COMTRADE_TOKEN'),
  secret_key = Sys.getenv('DO_COMTRADE_SECRET'),
  scheme = "https",
  endpoint_override = "ams3.digitaloceanspaces.com"
)

# space$ls('uncomtrade', recursive = T)

copy_files("hs-rev1992/parquet/", space$path("uncomtrade/hs-rev1992"))
copy_files("sitc-rev2/parquet/", space$path("uncomtrade/sitc-rev2"))

# quick verification

open_dataset("sitc-rev2/parquet/",
             partitioning = c("Year", "Trade Flow", "Reporter ISO")) %>%
  filter(Year == 2019, `Trade Flow` == "Export", `Reporter ISO` == "CHL") %>%
  collect()

open_dataset(space$path("uncomtrade/sitc-rev2/"),
             partitioning = c("Year", "Trade Flow", "Reporter ISO")) %>%
  filter(Year == 2019, `Trade Flow` == "Export", `Reporter ISO` == "CHL") %>%
  collect()
