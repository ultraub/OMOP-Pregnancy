#!/usr/bin/env Rscript

#' Test Full Pregnancy Identification Pipeline
#'
#' Runs the complete pipeline: Extraction -> HIP -> PPS -> ESD -> Merge -> Output

library(DatabaseConnector)
library(SqlRender)
library(dplyr)
library(lubridate)

# Load environment variables
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
  message("✓ Loaded environment variables")
}

# Source all R files
source_dir <- function(path) {
  files <- list.files(path, pattern = "\\.R$", recursive = TRUE, full.names = TRUE)
  for (file in files) {
    tryCatch({
      source(file)
    }, error = function(e) {
      warning(sprintf("Failed to load %s: %s", basename(file), e$message))
    })
  }
}

message("========================================")
message("Full Pregnancy Pipeline Test")
message("========================================")
message(sprintf("Test Date: %s", Sys.Date()))
message("")

# Load all components
message("Loading R components...")
source_dir("R/00_concepts")
source_dir("R/01_extraction")
source_dir("R/02_algorithms")
source_dir("R/03_results")
source_dir("R/03_utilities")

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

# Run full pipeline
tryCatch({
  
  message("\n==== PHASE 1: DATABASE CONNECTION ====")
  connection <- connect(connectionDetails)
  message("✓ Connected to Azure SQL Server")
  
  message("\n==== PHASE 2: LOAD CONCEPTS ====")
  concepts <- load_concept_sets()
  
  # Enrich with domains
  concepts$hip_concepts <- enrich_concepts_with_domains(
    concepts$hip_concepts,
    connection,
    cdm_schema
  )
  
  message(sprintf("✓ Loaded %d HIP concepts", nrow(concepts$hip_concepts)))
  message(sprintf("✓ Loaded %d PPS concepts", nrow(concepts$pps_concepts)))
  message(sprintf("✓ Loaded %d outcome categories", nrow(concepts$matcho_limits)))
  
  message("\n==== PHASE 3: EXTRACT COHORT (with temp tables) ====")
  
  start_time <- Sys.time()
  
  cohort_data <- extract_pregnancy_cohort(
    connection = connection,
    cdm_schema = cdm_schema,
    hip_concepts = concepts$hip_concepts,
    pps_concepts = concepts$pps_concepts,
    min_age = 15,
    max_age = 56,
    use_temp_tables = TRUE
  )
  
  end_time <- Sys.time()
  extraction_time <- as.numeric(difftime(end_time, start_time, units = "secs"))
  
  message(sprintf("\n✓ Extraction completed in %.1f seconds", extraction_time))
  message(sprintf("  - Persons: %d", nrow(cohort_data$persons)))
  message(sprintf("  - Conditions: %d", nrow(cohort_data$conditions)))
  message(sprintf("  - Procedures: %d", nrow(cohort_data$procedures)))
  message(sprintf("  - Observations: %d", nrow(cohort_data$observations)))
  message(sprintf("  - Measurements: %d", nrow(cohort_data$measurements)))
  message(sprintf("  - Gestational timing records: %d", nrow(cohort_data$gestational_timing)))
  
  # Track overall timing
  pipeline_start <- Sys.time()
  
  message("\n==== PHASE 4: RUN HIP ALGORITHM ====")
  
  hip_start <- Sys.time()
  hip_episodes <- run_hip_algorithm(
    cohort_data,
    concepts$matcho_limits,
    concepts$matcho_outcome_limits
  )
  hip_time <- as.numeric(difftime(Sys.time(), hip_start, units = "secs"))
  
  if (nrow(hip_episodes) > 0) {
    message(sprintf("✓ HIP identified %d episodes in %.1f seconds", 
                    nrow(hip_episodes), hip_time))
    
    hip_summary <- hip_episodes %>%
      count(outcome_category) %>%
      arrange(desc(n))
    
    message("\nHIP Episode Outcomes:")
    print(hip_summary)
  } else {
    message("  No HIP episodes identified")
  }
  
  message("\n==== PHASE 5: RUN PPS ALGORITHM ====")
  
  pps_start <- Sys.time()
  pps_episodes <- run_pps_algorithm(
    cohort_data,
    concepts$pps_concepts
  )
  pps_time <- as.numeric(difftime(Sys.time(), pps_start, units = "secs"))
  
  if (nrow(pps_episodes) > 0) {
    message(sprintf("✓ PPS identified %d episodes in %.1f seconds", 
                    nrow(pps_episodes), pps_time))
    
    pps_summary <- pps_episodes %>%
      count(outcome_category) %>%
      arrange(desc(n))
    
    message("\nPPS Episode Outcomes:")
    print(pps_summary)
  } else {
    message("  No PPS episodes identified")
  }
  
  message("\n==== PHASE 6: MERGE HIP AND PPS EPISODES ====")
  
  # First merge HIP and PPS episodes to handle overlaps
  merge_start <- Sys.time()
  merged_episodes <- merge_pregnancy_episodes(
    hip_episodes,
    pps_episodes,
    cohort_data
  )
  merge_time <- as.numeric(difftime(Sys.time(), merge_start, units = "secs"))
  
  if (nrow(merged_episodes) > 0) {
    message(sprintf("✓ Merged to %d unique episodes in %.1f seconds", 
                    nrow(merged_episodes), merge_time))
    
    merged_summary <- merged_episodes %>%
      count(algorithm_used) %>%
      arrange(desc(n))
    
    message("\nEpisodes by Algorithm Source:")
    print(merged_summary)
  } else {
    message("  No episodes after merging")
  }
  
  message("\n==== PHASE 7: CALCULATE ESTIMATED START DATES (ESD) ====")
  
  esd_start <- Sys.time()
  if (nrow(merged_episodes) > 0) {
    final_episodes <- calculate_estimated_start_dates(
      merged_episodes,
      cohort_data,
      concepts$pps_concepts
    )
  } else {
    final_episodes <- data.frame()
  }
  esd_time <- as.numeric(difftime(Sys.time(), esd_start, units = "secs"))
  
  if (nrow(final_episodes) > 0) {
    message(sprintf("✓ ESD refined %d episodes in %.1f seconds", 
                    nrow(final_episodes), esd_time))
    
    # Show precision categories if available
    if ("precision_category" %in% names(final_episodes)) {
      precision_summary <- final_episodes %>%
        count(precision_category) %>%
        arrange(desc(n))
      
      message("\nESD Precision Categories:")
      print(precision_summary)
    }
  } else {
    message("  No episodes to refine with ESD")
  }
  
  # Final summary
  if (nrow(final_episodes) > 0) {
    final_summary <- final_episodes %>%
      count(outcome_category, algorithm_used) %>%
      arrange(outcome_category, desc(n))
    
    message("\nFinal Episode Summary by Algorithm:")
    print(final_summary)
    
    # Overall outcome summary
    overall_summary <- final_episodes %>%
      count(outcome_category) %>%
      arrange(desc(n))
    
    message("\nOverall Outcome Summary:")
    print(overall_summary)
    
    # Person-level summary
    person_summary <- final_episodes %>%
      group_by(person_id) %>%
      summarise(
        episodes = n(),
        first_episode = min(episode_start_date),
        last_episode = max(episode_end_date)
      )
    
    message(sprintf("\n✓ Found pregnancies for %d unique persons", 
                    nrow(person_summary)))
    message(sprintf("  - Average episodes per person: %.2f", 
                    mean(person_summary$episodes)))
    message(sprintf("  - Max episodes per person: %d", 
                    max(person_summary$episodes)))
  }
  
  # Total pipeline time
  pipeline_time <- as.numeric(difftime(Sys.time(), pipeline_start, units = "secs"))
  
  message("\n==== PIPELINE COMPLETE ====")
  message(sprintf("Total pipeline time: %.1f seconds", pipeline_time))
  message(sprintf("  - Extraction: %.1f seconds", extraction_time))
  message(sprintf("  - HIP Algorithm: %.1f seconds", hip_time))
  message(sprintf("  - PPS Algorithm: %.1f seconds", pps_time))
  message(sprintf("  - Episode Merging: %.1f seconds", merge_time))
  message(sprintf("  - ESD Refinement: %.1f seconds", esd_time))
  
  # Save results
  message("\n==== PHASE 8: SAVE RESULTS ====")
  
  # Validate date columns before saving
  if (nrow(final_episodes) > 0) {
    # Ensure all date columns are properly formatted as Date objects
    final_episodes <- final_episodes %>%
      mutate(
        episode_start_date = as.Date(episode_start_date),
        episode_end_date = as.Date(episode_end_date)
      )
    
    # Check for epoch conversion issue (dates ~70 years in the past)
    # If the calculated gestational age doesn't match, we likely have the epoch issue
    final_episodes <- final_episodes %>%
      mutate(
        calc_gest_days = as.numeric(episode_end_date - episode_start_date),
        has_epoch_issue = (
          # Classic pattern: start after 1970, end before 1960
          (year(episode_start_date) > 1970 & year(episode_end_date) < 1960) |
          # Or: negative gestational age (end before start)
          (calc_gest_days < 0) |
          # Or: gestational age way off (>1000 days difference)
          (abs(calc_gest_days - gestational_age_days) > 1000)
        )
      )
    
    epoch_issue <- final_episodes %>%
      filter(has_epoch_issue)
    
    if (nrow(epoch_issue) > 0) {
      message(sprintf("Detected epoch conversion issue in %d episodes, applying correction...", 
                      nrow(epoch_issue)))
      
      # The issue is that dates are off by exactly 25569 days (accounting for leap years)
      # Correct the episode_end_date by adding this offset
      final_episodes <- final_episodes %>%
        mutate(
          episode_end_date = if_else(
            has_epoch_issue,
            episode_end_date + 25569,  # Add the epoch difference (corrected)
            episode_end_date
          )
        ) %>%
        select(-has_epoch_issue, -calc_gest_days)
      
      message("Date correction applied")
    }
    
    # Check for any remaining invalid dates
    invalid_dates <- final_episodes %>%
      filter(
        is.na(episode_start_date) | is.na(episode_end_date) |
        episode_start_date < as.Date("1900-01-01") | 
        episode_end_date < as.Date("1900-01-01") |
        episode_start_date > Sys.Date() |
        episode_end_date > Sys.Date() + 365
      )
    
    if (nrow(invalid_dates) > 0) {
      warning(sprintf("Found %d episodes with invalid dates", nrow(invalid_dates)))
      print(head(invalid_dates))
    }
    
    # Verify date format
    message(sprintf("Date column types: start=%s, end=%s", 
                    class(final_episodes$episode_start_date)[1],
                    class(final_episodes$episode_end_date)[1]))
  }
  
  output_dir <- "output"
  if (!dir.exists(output_dir)) {
    dir.create(output_dir)
  }
  
  # Save final episodes
  write.csv(
    final_episodes,
    file.path(output_dir, sprintf("pregnancy_episodes_%s.csv", Sys.Date())),
    row.names = FALSE
  )
  
  # Save summary
  summary_data <- list(
    extraction_time = extraction_time,
    persons_count = nrow(cohort_data$persons),
    hip_episodes = nrow(hip_episodes),
    pps_episodes = nrow(pps_episodes),
    merged_episodes = nrow(merged_episodes),
    final_episodes = nrow(final_episodes),
    unique_persons = if(exists("person_summary")) nrow(person_summary) else 0,
    pipeline_time = pipeline_time
  )
  
  saveRDS(
    summary_data,
    file.path(output_dir, sprintf("pipeline_summary_%s.rds", Sys.Date()))
  )
  
  message("✓ Results saved to output directory")
  
  message("\n✅ FULL PIPELINE COMPLETED SUCCESSFULLY!")
  
}, error = function(e) {
  message("\n❌ Pipeline failed with error:")
  message(e$message)
  traceback()
}, finally = {
  if (exists("connection")) {
    DatabaseConnector::disconnect(connection)
    message("\n✓ Database connection closed")
  }
})

message("\n========================================")
message("Pipeline Test Complete")
message("========================================")