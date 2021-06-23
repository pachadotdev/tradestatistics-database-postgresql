# classification ----------------------------------------------------------

classification <- ifelse(dataset < 6, "hs", "sitc")

# years by classification -------------------------------------------------

revision <- switch (
  dataset,
  `1` = 1992,
  `2` = 1996,
  `3` = 2002,
  `4` = 2007,
  `5` = 2012,
  `6` = 1,
  `7` = 2,
  `8` = 3,
  `9` = 4
)

revision2 <- switch (
  dataset,
  `1` = 1988,
  `2` = 1996,
  `3` = 2002,
  `4` = 2007,
  `5` = 2012,
  `6` = 1962,
  `7` = 1976,
  `8` = 1988,
  `9` = 2007
)

classification2 <- switch (
  dataset,
  `1` = "H0",
  `2` = "H1",
  `3` = "H2",
  `4` = "H3",
  `5` = "H4",
  `6` = "S1",
  `7` = "S2",
  `8` = "S3",
  `9` = "S4"
)

max_year <- 2020
years <- revision2:max_year
