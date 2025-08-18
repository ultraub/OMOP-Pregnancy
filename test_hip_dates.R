#!/usr/bin/env Rscript

# Test script to trace date handling through HIP algorithm

library(DatabaseConnector)
library(SqlRender)
library(dplyr)
library(lubridate)

# Load environment
if (file.exists(".env")) {
  env_lines <- readLines(".env")
  for (line in env_lines) {
    line <- trimws(line)
    if (nchar(line) > 0 && !startsWith(line, "#")) {
      parts <- strsplit(line, "=", fixed = TRUE)[[1]]
      if (length(parts) >= 2) {
        key <- trimws(parts[1])
        value <- trimws(paste(parts[-1], collapse = "="))
        do.call(Sys.setenv, setNames(list(value), key))
      }
    }
  }
}

# Source necessary files
source("R/00_concepts/load_concepts.R")
source("R/01_extraction/extract_cohort.R")
source("R/01_extraction/type_enforcement.R")
source("R/02_algorithms/hip_algorithm.R")
source("R/03_utilities/temp_table_utils.R")
source("R/03_utilities/utility_functions.R")

# Connection setup
server <- Sys.getenv("DB_SERVER")
database <- Sys.getenv("DB_DATABASE")
user <- Sys.getenv("DB_USER")
password <- Sys.getenv("DB_PASSWORD")
port <- as.numeric(Sys.getenv("DB_PORT", "1433"))
cdm_schema <- Sys.getenv("CDM_SCHEMA", "dbo")

jdbc_folder <- "jdbc_drivers"
jdbc_url <- sprintf(
  "jdbc:sqlserver://%s:%d;database=%s;encrypt=true;trustServerCertificate=true;",
  server, port, database
)

connectionDetails <- createConnectionDetails(
  dbms = "sql server",
  connectionString = jdbc_url,
  user = user,
  password = password,
  pathToDriver = jdbc_folder
)

tryCatch({
  # Connect
  connection <- connect(connectionDetails)
  
  # Load concepts
  concepts <- load_concept_sets()
  
  # Extract a small sample
  cat("\nExtracting small cohort sample...\n")
  cohort_data <- extract_pregnancy_cohort(
    connection = connection,
    cdm_schema = cdm_schema,
    hip_concepts = concepts$hip_concepts,
    pps_concepts = concepts$pps_concepts,
    min_age = 15,
    max_age = 56,
    use_temp_tables = TRUE
  )
  
  # Take just first 100 conditions for testing
  test_conditions <- head(cohort_data$conditions, 100)
  
  cat("\nSample of conditions data:\n")
  print(head(test_conditions))
  
  cat("\nChecking event_date column:\n")
  cat("Class:", class(test_conditions$event_date), "\n")
  cat("Sample values:\n")
  print(head(test_conditions$event_date))
  
  # Test HIP algorithm with debugging
  cat("\n\nTesting HIP algorithm with debugging...\n")
  
  # Run just the first step of HIP
  lb_sb_data <- test_conditions %>%
    filter(category %in% c("LB", "SB"))
  
  if (nrow(lb_sb_data) > 0) {
    cat("\nLB/SB data before grouping:\n")
    print(head(lb_sb_data[, c("person_id", "event_date", "category")]))
    cat("event_date class:", class(lb_sb_data$event_date), "\n")
    
    # Group episodes
    episodes <- lb_sb_data %>%
      arrange(person_id, event_date) %>%
      group_by(person_id) %>%
      mutate(
        days_since_last = as.numeric(event_date - lag(event_date)),
        new_episode = is.na(days_since_last) | days_since_last >= 168,
        episode_id = cumsum(new_episode)
      ) %>%
      group_by(person_id, episode_id) %>%
      summarise(
        outcome_date = max(as.Date(event_date)),
        outcome_category = first(category),
        n_visits = n(),
        .groups = "drop"
      ) %>%
      ungroup()
    
    cat("\nEpisodes after grouping:\n")
    print(head(episodes))
    cat("outcome_date class:", class(episodes$outcome_date), "\n")
    cat("outcome_date numeric values:", head(as.numeric(episodes$outcome_date)), "\n")
    
    # Now test the date calculation
    if (nrow(episodes) > 0) {
      test_calc <- episodes %>%
        head(5) %>%
        mutate(
          # Test different calculation methods
          test1 = outcome_date - 280,
          test1_class = class(test1)[1],
          test1_numeric = as.numeric(test1),
          
          test2 = as.Date(outcome_date) - 280,
          test2_class = class(test2)[1],
          test2_numeric = as.numeric(test2),
          
          test3 = as.Date(outcome_date - 280),
          test3_class = class(test3)[1],
          test3_numeric = as.numeric(test3)
        )
      
      cat("\n\nDate calculation tests:\n")
      print(test_calc[, c("outcome_date", "test1", "test2", "test3")])
      
      cat("\n\nNumeric values:\n")
      print(test_calc[, c("outcome_date", "test1_numeric", "test2_numeric", "test3_numeric")])
    }
  }
  
}, error = function(e) {
  cat("\nError:", e$message, "\n")
  traceback()
}, finally = {
  if (exists("connection")) {
    disconnect(connection)
  }
})