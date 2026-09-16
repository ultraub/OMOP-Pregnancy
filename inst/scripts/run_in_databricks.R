# Run the OMOP pregnancy pipeline inside Databricks
#
# Runs on the notebook's (or job's) Spark session: no JDBC driver, no token.
# Check the repository out as a Databricks Repo, then in an R notebook cell:
#
#   Sys.setenv(
#     OMOP_REPO_PATH    = "/Workspace/Repos/<user>/OMOP-Pregnancy",
#     CDM_SCHEMA        = "omop.data",
#     VOCABULARY_SCHEMA = "omop.vocabulary",
#     RESULTS_SCHEMA    = "my_project.results"         # optional
#   )
#   source(file.path(Sys.getenv("OMOP_REPO_PATH"), "inst/scripts/run_in_databricks.R"))
#
# Output files go to <repo>/output by default (the folder is git-ignored);
# set OUTPUT_FOLDER to write somewhere else, e.g. a Unity Catalog volume.
#
# Packages needed on the cluster: sparklyr, DBI, SqlRender (needs rJava; the
# Databricks Runtime provides Java), dplyr, lubridate, readr.
# DatabaseConnector is NOT needed for this path.

repo_path <- Sys.getenv("OMOP_REPO_PATH", unset = getwd())
setwd(repo_path)

suppressPackageStartupMessages({
  library(dplyr)
  library(lubridate)
  library(readr)
})

for (dir in c("R/00_connection", "R/00_concepts", "R/01_extraction",
              "R/02_algorithms", "R/03_results", "R/03_utilities")) {
  for (f in list.files(dir, pattern = "\\.R$", full.names = TRUE)) source(f)
}
source("R/main.R")

cdm_schema        <- Sys.getenv("CDM_SCHEMA")
vocabulary_schema <- Sys.getenv("VOCABULARY_SCHEMA", unset = cdm_schema)
results_schema    <- Sys.getenv("RESULTS_SCHEMA", unset = "")
output_folder     <- Sys.getenv("OUTPUT_FOLDER", unset = file.path(repo_path, "output"))
min_age           <- as.numeric(Sys.getenv("MIN_AGE", unset = "15"))
max_age           <- as.numeric(Sys.getenv("MAX_AGE", unset = "56"))

if (cdm_schema == "") stop("Set CDM_SCHEMA to the catalog.schema of the CDM tables, e.g. omop.data")
if (results_schema == "") results_schema <- NULL
if (output_folder == "") output_folder <- NULL
if (!is.null(output_folder) && !dir.exists(output_folder)) dir.create(output_folder, recursive = TRUE)

connection <- create_spark_connection(
  cdm_schema = cdm_schema,
  vocabulary_schema = vocabulary_schema,
  results_schema = results_schema
)
print(connection)

episodes <- run_pregnancy_identification(
  connection = connection,
  cdm_database_schema = cdm_schema,
  vocabulary_database_schema = vocabulary_schema,
  results_database_schema = results_schema,
  output_folder = output_folder,
  min_age = min_age,
  max_age = max_age
)

message(sprintf("Pipeline finished: %d episodes for %d persons",
                nrow(episodes), dplyr::n_distinct(episodes$person_id)))
