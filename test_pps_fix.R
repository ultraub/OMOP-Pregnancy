#!/usr/bin/env Rscript

cat("Testing PPS Algorithm Fix\n")
cat("=========================\n\n")

# Set Java environment
Sys.setenv(JAVA_HOME='/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home')
Sys.setenv(DATABASECONNECTOR_JAR_FOLDER='inst/jdbc')

# Load libraries
suppressPackageStartupMessages({
  library(DatabaseConnector)
  library(dplyr)
  library(dbplyr)
  library(readxl)
  library(purrr)
  library(tidyr)
  library(lubridate)
})

# Source functions
source('R/config.R')
source('R/connection.R')
source('R/pps_algorithm.R')

# Load configuration
config <- load_config('inst/config/config.yml')

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
visit_occurrence_tbl <- get_cdm_table(connection, 'visit_occurrence')

# Load PPS concepts
PPS_concepts <- read_excel('inst/extdata/PPS_concepts.xlsx')

cat("Testing PPS algorithm...\n")

# Get input GT concepts
input_GT_concepts_df <- input_GT_concepts(
  condition_occurrence_tbl, procedure_occurrence_tbl,
  observation_tbl, measurement_tbl, visit_occurrence_tbl, 
  PPS_concepts
)

# Count concepts
concept_count <- input_GT_concepts_df %>% count() %>% collect()
cat("  Found", concept_count$n, "timing concepts\n")

# Try to get PPS episodes - this is where the error was occurring
tryCatch({
  cat("\nAttempting to get PPS episodes...\n")
  pps_episodes <- get_PPS_episodes(
    input_GT_concepts_df, 
    PPS_concepts, 
    person_tbl, 
    config
  )
  
  cat("✓ SUCCESS! PPS episodes collected without error\n")
  cat("  Number of episodes:", nrow(pps_episodes), "\n")
  cat("  Number of unique persons:", n_distinct(pps_episodes$person_id), "\n")
  
  # Show sample of results
  cat("\nSample of PPS episodes:\n")
  print(head(pps_episodes %>% select(person_id, person_episode_number, domain_concept_start_date)))
  
}, error = function(e) {
  cat("✗ ERROR: ", e$message, "\n")
  cat("The fix did not resolve the issue.\n")
})

# Disconnect
disconnect(connection)
cat("\n✓ Test complete\n")