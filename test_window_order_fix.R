#!/usr/bin/env Rscript

cat("Testing window_order fix\n")
cat("========================\n\n")

# Set Java environment
Sys.setenv(JAVA_HOME='/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home')
Sys.setenv(DATABASECONNECTOR_JAR_FOLDER='inst/jdbc')

# Load libraries
suppressPackageStartupMessages({
  library(DatabaseConnector)
  library(dplyr)
  library(dbplyr)
  library(readxl)
  library(lubridate)
})

# Source functions
source('R/config.R')
source('R/connection.R')
source('R/hip_algorithm.R')

# Connection details
connectionDetails <- createConnectionDetails(
  dbms = "sql server",
  server = "ohdsicdmsqlserver.database.windows.net",
  user = "dbadmin",
  password = "hopkinsx93ewD",
  port = 1433,
  pathToDriver = "inst/jdbc",
  extraSettings = "database=tufts"
)

# Connect to database
connection <- connect(connectionDetails)
cat("✓ Connected to database\n\n")

# Set attributes
attr(connection, 'mode') <- 'generic'
attr(connection, 'dbms') <- 'sql server'
attr(connection, 'cdmDatabaseSchema') <- 'dbo'

# Get tables
person_tbl <- get_cdm_table(connection, 'person')
condition_occurrence_tbl <- get_cdm_table(connection, 'condition_occurrence')
procedure_occurrence_tbl <- get_cdm_table(connection, 'procedure_occurrence')
observation_tbl <- get_cdm_table(connection, 'observation')
measurement_tbl <- get_cdm_table(connection, 'measurement')

# Load HIP concepts
HIP_concepts <- read_excel('inst/extdata/HIP_concepts.xlsx')

# Test the initial_pregnant_cohort function
cat("Testing initial_pregnant_cohort function...\n")
config <- list(
  mode = "generic",
  concepts = list(
    gender_concepts = list(
      active_male_id = 8507,
      active_female_id = 8532
    )
  ),
  algorithm = list(
    hip = list(
      min_age = 15,
      max_age = 56
    )
  )
)

tryCatch({
  initial_cohort <- initial_pregnant_cohort(
    condition_occurrence_tbl,
    procedure_occurrence_tbl,
    observation_tbl,
    measurement_tbl,
    person_tbl,
    HIP_concepts,
    config
  )
  
  cohort_count <- initial_cohort %>%
    summarise(n_persons = n_distinct(person_id)) %>%
    collect()
  
  cat("✓ Initial cohort created successfully:", cohort_count$n_persons, "persons\n")
  
  # Test final_visits function
  cat("\nTesting final_visits function...\n")
  
  # Load Matcho limits
  Matcho_outcome_limits <- tribble(
    ~first_preg_category, ~outcome_preg_category, ~min_days,
    "LB", "LB", 154,
    "SB", "SB", 154,
    "AB", "AB", 42,
    "SA", "SA", 42,
    "ECT", "ECT", 42,
    "DELIV", "DELIV", 154
  )
  
  # Test with outcome categories
  outcome_categories <- c("LB", "SB", "AB", "SA", "ECT", "DELIV")
  
  final_visits_df <- final_visits(initial_cohort, Matcho_outcome_limits, outcome_categories)
  
  visits_count <- final_visits_df %>%
    count() %>%
    collect()
  
  cat("✓ Final visits processed successfully:", visits_count$n, "visits\n")
  
}, error = function(e) {
  cat("✗ ERROR:", e$message, "\n")
  cat("\nStack trace:\n")
  print(traceback())
})

# Disconnect
disconnect(connection)
cat("\n✓ Test complete\n")