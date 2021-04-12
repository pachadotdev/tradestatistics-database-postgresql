raw_dir <- sprintf("%s-rev%s", classification, revision)
try(dir.create(raw_dir))

raw_dir_zip <- sprintf("%s/%s", raw_dir, "zip")
try(dir.create(raw_dir_zip))

raw_dir_parquet <- str_replace(raw_dir_zip, "zip", "parquet")
try(dir.create(raw_dir_parquet))
