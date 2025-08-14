# Test OMOPPregnancy with actual database
suppressPackageStartupMessages({
  library(DBI)
  library(odbc)
  library(dplyr)
  library(dbplyr)
  library(magrittr)
})

# Source package functions
source("R/config.R")
source("R/connection.R")
source("R/query_utils.R")
source("R/sql_functions.R")
source("R/hip_algorithm.R")
source("R/pps_algorithm.R")
source("R/merge_episodes.R")
source("R/esd_algorithm.R")

cat("========================================\n")
cat("Testing OMOPPregnancy with Tufts Database\n")
cat("========================================\n\n")

# Connect to database
con <- dbConnect(
  odbc::odbc(),
  Driver = "ODBC Driver 17 for SQL Server",
  Server = "ohdsicdmsqlserver.database.windows.net",
  Database = "tufts",
  UID = "dbadmin",
  PWD = "",
  Port = 1433
)

# Check CDM tables
cat("--- Checking CDM Tables ---\n")
cdm_tables <- c("person", "procedure_occurrence", "measurement", 
                "observation", "condition_occurrence", "visit_occurrence")

for (table_name in cdm_tables) {
  tbl_ref <- tbl(con, table_name)
  count <- tbl_ref %>% 
    head(1) %>% 
    collect() %>% 
    nrow()
  if (count > 0) {
    cat("✓", table_name, "exists\n")
  }
}

# Check for women
cat("\n--- Checking Demographics ---\n")
person_tbl <- tbl(con, "person")
gender_dist <- person_tbl %>%
  group_by(gender_concept_id) %>%
  count() %>%
  collect()

print(gender_dist)

# Check for pregnancy data
cat("\n--- Checking for Pregnancy Data ---\n")
measurement_tbl <- tbl(con, "measurement")

# Gestational age concepts
gest_concepts <- c(3002209, 3048230, 3012266)
gest_count <- measurement_tbl %>%
  filter(measurement_concept_id %in% gest_concepts) %>%
  count() %>%
  collect() %>%
  pull(n)

cat("Gestational age measurements:", gest_count, "\n")

# Check pregnancy procedures
procedure_tbl <- tbl(con, "procedure_occurrence")
pregnancy_proc_concepts <- c(4013024, 4145193, 4047364)
preg_proc_count <- procedure_tbl %>%
  filter(procedure_concept_id %in% pregnancy_proc_concepts) %>%
  count() %>%
  collect() %>%
  pull(n)

cat("Pregnancy procedure records:", preg_proc_count, "\n")

# Load config
cat("\n--- Testing Package Functions ---\n")
config <- load_config(mode = "generic")
cat("✓ Config loaded\n")

# Test with a simple cohort
cat("\n--- Testing Initial Cohort ---\n")
test_cohort <- person_tbl %>%
  filter(gender_concept_id == 8532) %>%  # Female
  head(100) %>%
  collect()

cat("Test cohort size:", nrow(test_cohort), "women\n")

dbDisconnect(con)
cat("\n✓ Test complete\n")
