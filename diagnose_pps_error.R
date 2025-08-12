#!/usr/bin/env Rscript

# Diagnostic script to find the root cause of the PPS SQL error

cat("==================================================\n")
cat("PPS Algorithm SQL Error Diagnosis\n")
cat("==================================================\n\n")

# Set Java environment
Sys.setenv(JAVA_HOME='/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home')
Sys.setenv(DATABASECONNECTOR_JAR_FOLDER='inst/jdbc')

# Load required libraries
library(DatabaseConnector)
library(dplyr)
library(dbplyr)
library(readxl)
library(purrr)
library(tidyr)
library(lubridate)

# Source the package functions
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

cat("Testing PPS algorithm step by step...\n\n")

# Step 1: Get input GT concepts
cat("Step 1: Getting input GT concepts...\n")
input_GT_concepts_df <- input_GT_concepts(
  condition_occurrence_tbl, procedure_occurrence_tbl,
  observation_tbl, measurement_tbl, visit_occurrence_tbl, 
  PPS_concepts
)
cat("  ✓ Input GT concepts created\n")

# Count the concepts
concept_count <- input_GT_concepts_df %>% count() %>% collect()
cat("  Found", concept_count$n, "timing concepts\n\n")

# Step 2: Test the problematic query piece by piece
cat("Step 2: Building the problematic query step by step...\n\n")

# Get configuration
male_concept_id <- 8507
min_age <- 15
max_age <- 56

# Step 2a: Filter for non-NA dates
cat("  2a. Filtering for non-NA dates...\n")
step1 <- filter(input_GT_concepts_df, !is.na(domain_concept_start_date))
tryCatch({
  count1 <- step1 %>% count() %>% collect()
  cat("     ✓ After filter:", count1$n, "records\n")
}, error = function(e) {
  cat("     ✗ ERROR:", e$message, "\n")
})

# Step 2b: Join with PPS_concepts
cat("  2b. Joining with PPS_concepts...\n")
step2 <- step1 %>%
  left_join(PPS_concepts, by = "domain_concept_id", copy = TRUE)
tryCatch({
  count2 <- step2 %>% count() %>% collect()
  cat("     ✓ After PPS join:", count2$n, "records\n")
}, error = function(e) {
  cat("     ✗ ERROR:", e$message, "\n")
})

# Step 2c: Create person subset
cat("  2c. Creating person subset...\n")
person_subset <- person_tbl %>%
  select(person_id, gender_concept_id, year_of_birth, day_of_birth, month_of_birth)
tryCatch({
  # Don't collect, just check if query builds
  query <- show_query(person_subset)
  cat("     ✓ Person subset query builds successfully\n")
}, error = function(e) {
  cat("     ✗ ERROR:", e$message, "\n")
})

# Step 2d: Join with person
cat("  2d. Joining with person table...\n")
step3 <- step2 %>%
  inner_join(person_subset, by = "person_id")
tryCatch({
  # Show the SQL being generated
  cat("\n     Generated SQL (preview):\n")
  query_text <- capture.output(show_query(step3))
  cat(paste("     ", head(query_text, 10), collapse = "\n"), "\n")
  
  count3 <- step3 %>% count() %>% collect()
  cat("     ✓ After person join:", count3$n, "records\n")
}, error = function(e) {
  cat("     ✗ ERROR at person join:", e$message, "\n")
  cat("     This is likely where the issue is!\n")
})

# Step 2e: Add date calculations
cat("  2e. Adding date calculations...\n")

# First try: All in one mutate (original approach)
cat("\n  Testing original approach (all in one mutate)...\n")
step4_original <- step3 %>%
  mutate(
    day_of_birth = if_else(is.na(day_of_birth), 1, day_of_birth),
    month_of_birth = if_else(is.na(month_of_birth), 1, month_of_birth),
    date_of_birth = sql("DATEFROMPARTS(year_of_birth, month_of_birth, day_of_birth)"),
    date_diff = sql("DATEDIFF(day, date_of_birth, domain_concept_start_date)"),
    age = date_diff / 365
  )
tryCatch({
  query_text <- capture.output(show_query(step4_original))
  cat("     SQL generates successfully\n")
  count4 <- step4_original %>% count() %>% collect()
  cat("     ✓ Works! Count:", count4$n, "\n")
}, error = function(e) {
  cat("     ✗ ERROR:", e$message, "\n")
})

# Second try: Separated mutates (current approach)
cat("\n  Testing current approach (separated mutates)...\n")
step4_separated <- step3 %>%
  mutate(
    day_of_birth = if_else(is.na(day_of_birth), 1, day_of_birth),
    month_of_birth = if_else(is.na(month_of_birth), 1, month_of_birth)
  ) %>%
  mutate(
    date_of_birth = sql("DATEFROMPARTS(year_of_birth, month_of_birth, day_of_birth)")
  ) %>%
  mutate(
    date_diff = sql("DATEDIFF(day, date_of_birth, domain_concept_start_date)"),
    age = date_diff / 365
  )
tryCatch({
  count4 <- step4_separated %>% count() %>% collect()
  cat("     ✓ Works! Count:", count4$n, "\n")
}, error = function(e) {
  cat("     ✗ ERROR:", e$message, "\n")
})

# Step 2f: Add filters
cat("  2f. Adding age and gender filters...\n")
step5 <- step4_separated %>%
  filter(
    gender_concept_id != male_concept_id,
    age >= min_age,
    age < max_age
  )
tryCatch({
  count5 <- step5 %>% count() %>% collect()
  cat("     ✓ After filters:", count5$n, "records\n")
}, error = function(e) {
  cat("     ✗ ERROR:", e$message, "\n")
})

# Step 2g: Remove columns
cat("  2g. Removing columns...\n")

# Test different select approaches
cat("\n  Testing select with individual columns...\n")
step6_individual <- step5 %>%
  select(-year_of_birth, -month_of_birth, -day_of_birth, -date_diff, -gender_concept_id)
tryCatch({
  count6 <- step6_individual %>% count() %>% collect()
  cat("     ✓ Works! Count:", count6$n, "\n")
}, error = function(e) {
  cat("     ✗ ERROR with individual select:", e$message, "\n")
})

# Test with select everything except
cat("\n  Testing select with everything except specific columns...\n")
step6_except <- step5 %>%
  select(person_id, domain_concept_start_date, domain_concept_id, 
         min_month, max_month, age, date_of_birth)
tryCatch({
  count6 <- step6_except %>% count() %>% collect()
  cat("     ✓ Works! Count:", count6$n, "\n")
}, error = function(e) {
  cat("     ✗ ERROR with positive select:", e$message, "\n")
})

# Final test: Try to collect the full query
cat("\n  Final test: Collecting full query...\n")
patients_with_preg_concepts <- step5 %>%
  select(-year_of_birth, -month_of_birth, -day_of_birth, -date_diff, -gender_concept_id)

tryCatch({
  result <- collect(patients_with_preg_concepts)
  cat("     ✓ SUCCESS! Collected", nrow(result), "rows\n")
  cat("\n     Column names in result:\n")
  cat("     ", paste(names(result), collapse = ", "), "\n")
}, error = function(e) {
  cat("     ✗ FAILED:", e$message, "\n")
  cat("\n     Trying to show the SQL that fails:\n")
  tryCatch({
    sql_query <- capture.output(show_query(patients_with_preg_concepts))
    cat(paste(sql_query, collapse = "\n"), "\n")
  }, error = function(e2) {
    cat("     Could not show query:", e2$message, "\n")
  })
})

# Disconnect
disconnect(connection)
cat("\n✓ Diagnosis complete\n")