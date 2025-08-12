#!/usr/bin/env Rscript

# Example script showing how to use .Renv for secure credential management

cat("==================================================\n")
cat("OMOP Pregnancy Package - Using Environment Variables\n")
cat("==================================================\n\n")

# Load required libraries
suppressPackageStartupMessages({
  library(DatabaseConnector)
  library(dplyr)
  library(dbplyr)
  library(readxl)
  library(lubridate)
})

# Source the package functions
source('R/load_env.R')
source('R/config.R')
source('R/connection.R')
source('R/hip_algorithm.R')
source('R/pps_algorithm.R')

# Load environment variables
cat("Loading environment variables from .Renv...\n")
load_env(verbose = TRUE)

# Get connection details from environment
cat("\nCreating database connection from environment variables...\n")
connectionDetails <- get_connection_from_env()

# Connect to database
connection <- connect(connectionDetails)
cat("✓ Connected to database\n\n")

# Set attributes from environment
attr(connection, 'mode') <- 'generic'
attr(connection, 'dbms') <- tolower(Sys.getenv("DB_DBMS", "sql server"))
attr(connection, 'cdmDatabaseSchema') <- Sys.getenv("CDM_SCHEMA", "dbo")

# Get tables
cat("Accessing OMOP CDM tables...\n")
person_tbl <- get_cdm_table(connection, 'person')
condition_occurrence_tbl <- get_cdm_table(connection, 'condition_occurrence')
procedure_occurrence_tbl <- get_cdm_table(connection, 'procedure_occurrence')
observation_tbl <- get_cdm_table(connection, 'observation')
measurement_tbl <- get_cdm_table(connection, 'measurement')
visit_occurrence_tbl <- get_cdm_table(connection, 'visit_occurrence')

# Quick test: count persons
person_count <- person_tbl %>% count() %>% collect()
cat(sprintf("  Found %d persons in database\n", person_count$n))

# Load concept sets
cat("\nLoading concept sets...\n")
HIP_concepts <- read_excel('inst/extdata/HIP_concepts.xlsx')
PPS_concepts <- read_excel('inst/extdata/PPS_concepts.xlsx')
cat(sprintf("  Loaded %d HIP concepts\n", nrow(HIP_concepts)))
cat(sprintf("  Loaded %d PPS concepts\n", nrow(PPS_concepts)))

# Quick test of HIP algorithm
cat("\nRunning quick HIP algorithm test...\n")
config <- list(
  mode = "generic",
  concepts = list(
    gender_concepts = list(
      active_male_id = 8507,
      active_female_id = 8532
    )
  )
)

# Get initial cohort
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

cat(sprintf("  ✓ HIP initial cohort: %d persons\n", cohort_count$n_persons))

# Quick test of PPS algorithm
cat("\nRunning quick PPS algorithm test...\n")
input_GT_concepts_df <- input_GT_concepts(
  condition_occurrence_tbl, 
  procedure_occurrence_tbl,
  observation_tbl, 
  measurement_tbl, 
  visit_occurrence_tbl, 
  PPS_concepts
)

concept_count <- input_GT_concepts_df %>% count() %>% collect()
cat(sprintf("  ✓ PPS timing concepts: %d\n", concept_count$n))

# Disconnect
disconnect(connection)
cat("\n✓ All tests completed successfully\n")
cat("Environment variables are working correctly!\n")