# Open uncomtrade-datasets-arrow.Rproj before running this function

source("99-packages.R")

# uncomtrademisc doesn't work since UN COMTRADE API was replaced by
# UN COMTRADE PLUS

# here is how I used to download data from UN COMTRADE API until 2023-12-31
# this covers all the classification systems used by countries reporting to
# the UN (i.e., if a country used HS92 in 2010, I download data for that year
# using HS92 even when UN COMTRADE converts it to HS07)

# no country reports to use SITC 4, so I don't download data for that
# classification

# HS 92/96/02/07/12/17

# data_downloading(postgres = T, token = T, dataset = 1, remove_old_files = 1, subset_years = 1988:2022, parallel = 2, skip_updates = T)
# data_downloading(postgres = T, token = T, dataset = 2, remove_old_files = 1, subset_years = 1996:2021, parallel = 2, skip_updates = F)
# data_downloading(postgres = T, token = T, dataset = 3, remove_old_files = 1, subset_years = 2002:2021, parallel = 2, skip_updates = F)
# data_downloading(postgres = T, token = T, dataset = 4, remove_old_files = 1, subset_years = 2007:2021, parallel = 2, skip_updates = F)
# data_downloading(postgres = T, token = T, dataset = 5, remove_old_files = 1, subset_years = 2012:2021, parallel = 2, skip_updates = F)
# data_downloading(postgres = T, token = T, dataset = 6, remove_old_files = 1, subset_years = 2017:2021, parallel = 2, skip_updates = F)

# SITC 1/2/3

# data_downloading(postgres = T, token = T, dataset = 7, remove_old_files = 1, subset_years = 1962:2021, parallel = 2, skip_updates = F)
# data_downloading(postgres = T, token = T, dataset = 8, remove_old_files = 1, subset_years = 1976:2021, parallel = 2, skip_updates = T)
# data_downloading(postgres = T, token = T, dataset = 9, remove_old_files = 1, subset_years = 1988:2021, parallel = 2, skip_updates = F)
