# UN COMTRADE Datasets in Arrow Parquet

Source the file `00-download-data.R`. The script asks you if you have already configures the environment variables for UN COMTRADE token.

## What does the script do?

Let's you select and download the complete yearly records under different trade classification:

  1. HS rev 1992 (1992-2020)
  2. HS rev 1996 (1996-2020)
  3. HS rev 2002 (2002-2020)
  4. HS rev 2007 (2007-2020)
  5. SITC rev 1 (1962-2020)
  6. SITC rev 2 (1976-2020)
  7. SITC rev 3 (1988-2020)
  8. SITC rev 4 (2007-2020)

Then the downloaded files are saved locally in ZIP (as they come from UN COMTRADE) and Parquet format.

## How is this done? 

The scripts complete the next steps:

  1. A prompt is shown asking you if you have already obtained and saved a token to be able to download files from UN COMTRADE, then you'll be asked which classification you want to download (e.g. HS92), and if you want replace the old files with newer ones if local files are older than the available versions from UN COMTRADE at the moment of running the scripts.
  2. A CSV file containing the downloaded files, indicating the local download date, when was the file uploaded the UN COMTRADE and the download link will be saved locally (e.g. see LINK).
  3. Another CSV file containing subset of the updated files shall be saved locally (e.g. see LINK).
  4. If you selected the option of replacing old files, the old ZIP and Parquet files for the different years shall be replaced for new files. The parquet files are created by extracting the CSV file for each year from the ZIP, and then reading it to save a Parquet version with minimal edition (just replacing NAs with "0-unspecified" for the hive-stlye partitioning). The CSV files (which are extracted one at a time) are deleted after the Parquet files for a year are created.
  5. If the Parquet files for a certain year are not present, the scripts shall create those files with the same changes as in point (4).
  
## Required free disk space

Depends on the classification. For example, SITC rev 2 (1976-2020) needs 20GB of free space for the process and 18GB afterwards:

* 7.1GB to download the ZIP files
* 10.6GB for the Parquet files, which need free space to be created year-by-year.
* Up to 2.7GB for each extracted CSV file for Parquet files creation

## Notes

The scripts require 7-Zip to be installed.
