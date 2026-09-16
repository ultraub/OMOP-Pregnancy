# Databricks notebook source
# MAGIC %md
# MAGIC # OMOP Pregnancy pipeline on Databricks
# MAGIC
# MAGIC Runs the pregnancy episode pipeline on this cluster's Spark session: no JDBC driver, no token.
# MAGIC Attach to an all-purpose cluster running a Databricks Runtime (not a SQL warehouse).
# MAGIC
# MAGIC This is the notebook form of `Databricks/run_pipeline.qmd`; the Repo opens this file as a notebook.
# MAGIC Fill in the configuration cell, then run the cells in order.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Packages
# MAGIC Installs anything missing. `SqlRender` needs `rJava`; if that step fails, install `rJava` on its own first.

# COMMAND ----------

needed <- c("sparklyr", "DBI", "SqlRender", "dplyr", "lubridate", "readr")
missing <- needed[!vapply(needed, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0) {
  message("Installing: ", paste(missing, collapse = ", "))
  install.packages(missing)
}
suppressPackageStartupMessages({
  library(dplyr)
  library(lubridate)
  library(readr)
})
cat("R", R.version.string, "\n")
cat("sparklyr", as.character(packageVersion("sparklyr")), "\n")
cat("SqlRender", as.character(packageVersion("SqlRender")), "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

repo_path         <- "/Workspace/Repos/<your user>/OMOP-Pregnancy"
cdm_schema        <- "omop.data"
vocabulary_schema <- "omop.vocabulary"
results_schema    <- NULL          # e.g. "my_project.results"; NULL skips the table
output_folder     <- file.path(repo_path, "output")   # git-ignored
min_age           <- 15
max_age           <- 56

stopifnot(dir.exists(repo_path))
setwd(repo_path)
if (!dir.exists(output_folder)) dir.create(output_folder, recursive = TRUE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load the pipeline code

# COMMAND ----------

for (dir in c("R/00_connection", "R/00_concepts", "R/01_extraction",
              "R/02_algorithms", "R/03_results", "R/03_utilities")) {
  for (f in list.files(dir, pattern = "\\.R$", full.names = TRUE)) source(f)
}
source("R/main.R")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Connect to the Spark session
# MAGIC If `create_spark_connection()` cannot attach on your runtime, run
# MAGIC `sc <- sparklyr::spark_connect(method = "databricks")` and pass `sc = sc`.

# COMMAND ----------

connection <- create_spark_connection(
  cdm_schema = cdm_schema,
  vocabulary_schema = vocabulary_schema,
  results_schema = results_schema
)
print(connection)
n_persons <- db_query(connection, paste0("SELECT COUNT(*) AS n FROM ", cdm_schema, ".person"))
cat("Persons in CDM:", format(n_persons$n[1], big.mark = ","), "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Concept sets

# COMMAND ----------

concepts <- load_concept_sets()
cat("HIP concepts:", nrow(concepts$hip_concepts), " PPS concepts:", nrow(concepts$pps_concepts), "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Extract records (the only database step)

# COMMAND ----------

start_time <- Sys.time()
cohort_data <- extract_pregnancy_cohort(
  connection = connection,
  cdm_schema = cdm_schema,
  vocabulary_schema = vocabulary_schema,
  hip_concepts = concepts$hip_concepts,
  pps_concepts = concepts$pps_concepts,
  min_age = min_age,
  max_age = max_age
)
cat(sprintf("Extraction took %.1f minutes\n", as.numeric(difftime(Sys.time(), start_time, units = "mins"))))
cat("Persons:", n_distinct(cohort_data$persons$person_id),
    " Conditions:", nrow(cohort_data$conditions), " Procedures:", nrow(cohort_data$procedures),
    " Observations:", nrow(cohort_data$observations), " Measurements:", nrow(cohort_data$measurements),
    " PPS timing:", nrow(cohort_data$gestational_timing), " ESD timing:", nrow(cohort_data$esd_timing), "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. HIP

# COMMAND ----------

hip_episodes <- run_hip_algorithm(cohort_data, concepts$matcho_limits, concepts$matcho_outcome_limits)
cat("HIP episodes:", nrow(hip_episodes), "\n")
hip_episodes %>% count(outcome_category, sort = TRUE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. PPS

# COMMAND ----------

pps_episodes <- run_pps_algorithm(cohort_data, concepts$pps_concepts)
cat("PPS episodes:", nrow(pps_episodes), "\n")
pps_episodes %>% count(outcome_category, sort = TRUE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Merge

# COMMAND ----------

merged_episodes <- merge_pregnancy_episodes(hip_episodes, pps_episodes)
cat("Merged episodes:", nrow(merged_episodes), "\n")
merged_episodes %>% count(algorithm_used, sort = TRUE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Estimated start dates and quality flags

# COMMAND ----------

final_episodes <- calculate_estimated_start_dates(merged_episodes, cohort_data, concepts$pps_concepts)
final_episodes <- add_episode_quality_metadata(final_episodes, concepts$matcho_limits)
cat("Episodes:", nrow(final_episodes), "for", n_distinct(final_episodes$person_id), "persons\n")
final_episodes %>% count(outcome_category, sort = TRUE)
final_episodes %>% count(precision_category, sort = TRUE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Save
# MAGIC CSV, RDS and a summary go to the output folder; with a results schema the `pregnancy_episodes` table is replaced.

# COMMAND ----------

save_results(
  episodes = final_episodes,
  output_folder = output_folder,
  connection = connection,
  results_schema = results_schema,
  start_time = start_time
)
list.files(output_folder)
cat(sprintf("Total runtime %.1f minutes\n", as.numeric(difftime(Sys.time(), start_time, units = "mins"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. A look at the output

# COMMAND ----------

final_episodes %>%
  select(person_id, episode_number, outcome_category, episode_start_date, episode_end_date,
         gestational_age_days, precision_category, algorithm_used) %>%
  head(20)
