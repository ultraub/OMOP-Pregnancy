# Test OMOPPregnancy with actual pregnancy data from Tufts
suppressPackageStartupMessages({
  library(DBI)
  library(odbc)
  library(dplyr)
  library(dbplyr)
  library(DatabaseConnector)
})

# Load package
devtools::load_all(".")

cat("========================================\n")
cat("Testing OMOPPregnancy with Real Data\n")
cat("========================================\n\n")

# Create connection using DatabaseConnector for compatibility
connectionDetails <- createConnectionDetails(
  dbms = "sql server",
  server = "ohdsicdmsqlserver.database.windows.net/tufts",
  user = "dbadmin",
  password = "",
  port = 1433
)

con <- connect(connectionDetails)

cat("✓ Connected to database\n\n")

# Found pregnancy data:
# - 170 condition records (concepts 439393, 4299535)
# - 1819 observation records (concept 4005823)
# Let's test with women who have these conditions

cat("--- Finding women with pregnancy conditions ---\n")

# Get women with pregnancy conditions
preg_women_query <- "
  SELECT DISTINCT p.person_id
  FROM person p
  INNER JOIN condition_occurrence co ON p.person_id = co.person_id
  WHERE p.gender_concept_id = 8532
    AND co.condition_concept_id IN (439393, 4299535)
"

preg_women <- querySql(con, preg_women_query)
cat("Found", nrow(preg_women), "women with pregnancy conditions\n")

if(nrow(preg_women) > 0) {
  cat("\n--- Testing HIP Algorithm ---\n")
  
  # Create config
  config <- list(
    cdm_database_schema = "dbo",
    results_database_schema = "dbo",
    cohort_table = "pregnancy_cohort",
    
    # Concept sets - use what we found
    pregnancy_concepts = list(
      delivery = c(4013024, 4145193, 4047364),
      ectopic = c(433260),
      abortion = c(439393),  # We have this!
      stillbirth = c(4014295),
      livebirth = c(4014295),
      pregnancy = c(4299535)  # We have this!
    ),
    
    gestation_concepts = list(
      gestational_age_weeks = c(3002209, 3048230, 3012266)
    ),
    
    algorithm_params = list(
      visit_buffer = 1,
      gestation_output = "EDD",
      gestation_period_length = 45
    )
  )
  
  # Try to run initial cohort
  tryCatch({
    cat("Creating initial pregnant cohort...\n")
    
    # Get person and condition tables
    person_tbl <- tbl(con, in_schema("dbo", "person"))
    condition_tbl <- tbl(con, in_schema("dbo", "condition_occurrence"))
    observation_tbl <- tbl(con, in_schema("dbo", "observation"))
    
    # Create cohort of women with pregnancy conditions
    cohort <- person_tbl %>%
      filter(gender_concept_id == 8532) %>%
      inner_join(
        condition_tbl %>%
          filter(condition_concept_id %in% c(439393, 4299535)),
        by = "person_id"
      ) %>%
      select(person_id, condition_start_date) %>%
      rename(visit_date = condition_start_date) %>%
      distinct() %>%
      compute()
    
    cohort_count <- cohort %>% count() %>% collect()
    cat("Initial cohort size:", cohort_count$n, "records\n")
    
    # Show sample
    sample_data <- cohort %>% head(5) %>% collect()
    cat("\nSample cohort data:\n")
    print(sample_data)
    
  }, error = function(e) {
    cat("Error in cohort creation:", e$message, "\n")
  })
  
  cat("\n--- Testing with Observations ---\n")
  
  # Also test with observation data (concept 4005823)
  tryCatch({
    obs_cohort <- person_tbl %>%
      filter(gender_concept_id == 8532) %>%
      inner_join(
        observation_tbl %>%
          filter(observation_concept_id == 4005823),
        by = "person_id"
      ) %>%
      select(person_id, observation_date) %>%
      rename(visit_date = observation_date) %>%
      distinct() %>%
      compute()
    
    obs_count <- obs_cohort %>% count() %>% collect()
    cat("Observation cohort size:", obs_count$n, "records\n")
    
  }, error = function(e) {
    cat("Error with observation cohort:", e$message, "\n")
  })
  
} else {
  cat("No women with pregnancy conditions found to test with\n")
}

# Disconnect
disconnect(con)
cat("\n✓ Test complete\n")