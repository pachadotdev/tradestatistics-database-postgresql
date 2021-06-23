has_token <- menu(
  c("yes", "no"),
  title = "Have you safely stored COMTRADE_TOKEN in .Renviron?",
  graphics = F
)

stopifnot(has_token == 1)

dataset <- menu(
  c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "HS rev 2012",
    "SITC rev 1", "SITC rev 2", "SITC rev 3", "SITC rev 4"),
  title = "Select dataset:",
  graphics = F
)

remove_old_files <- menu(
  c("yes", "no"),
  title = "Remove old files (y/n):",
  graphics = F
)
