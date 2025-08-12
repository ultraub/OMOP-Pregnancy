#!/usr/bin/env Rscript

# OMOP Concept Validation Script
# Validates pregnancy-related concepts against an OMOP CDM database

# Load required libraries
library(DatabaseConnector)
library(SqlRender)
library(dplyr)
library(yaml)

# Database connection parameters
# Note: User mentioned postgres but host suggests SQL Server
connection_details <- createConnectionDetails(
  dbms = "sql server",  # Change to "postgresql" if needed
  server = "ohdsicdmsqlserver.database.windows.net/tufts",
  user = "dbadmin",
  password = "hopkinsx93ewD",
  port = 1433
)

# Load concepts configuration
concepts_config_path <- system.file("config", "omop_concepts.yaml", 
                                   package = "OMOPPregnancy")
if (!file.exists(concepts_config_path)) {
  concepts_config_path <- "inst/config/omop_concepts.yaml"
}

if (file.exists(concepts_config_path)) {
  concepts <- yaml::read_yaml(concepts_config_path)
} else {
  stop("Cannot find OMOP concepts configuration file")
}

# Connect to database
cat("Connecting to database...\n")
con <- connect(connection_details)

# Function to validate a set of concept IDs
validate_concepts <- function(connection, concept_ids, concept_type) {
  if (length(concept_ids) == 0) return(NULL)
  
  sql <- "
  SELECT 
    @concept_ids as input_concept_id,
    c.concept_id as found_concept_id,
    c.concept_name,
    c.domain_id,
    c.vocabulary_id,
    c.concept_class_id,
    c.standard_concept,
    c.invalid_reason,
    CASE 
      WHEN c.concept_id IS NULL THEN 'MISSING'
      WHEN c.invalid_reason IS NOT NULL THEN 'INVALID'
      WHEN c.standard_concept != 'S' THEN 'NON_STANDARD'
      ELSE 'VALID'
    END as status
  FROM (
    SELECT CAST(concept_id AS INT) as concept_id 
    FROM (VALUES @concept_id_values) AS t(concept_id)
  ) input
  LEFT JOIN concept c ON input.concept_id = c.concept_id
  "
  
  # Create values string
  values_str <- paste0("(", concept_ids, ")", collapse = ",")
  
  sql <- SqlRender::render(sql,
    concept_ids = concept_ids[1],
    concept_id_values = values_str
  )
  
  result <- querySql(connection, sql)
  result$concept_type <- concept_type
  
  return(result)
}

# Validate gender concepts
cat("\nValidating gender concepts...\n")
gender_ids <- c(
  concepts$gender_concepts$male_concept_id,
  concepts$gender_concepts$female_concept_id,
  concepts$gender_concepts$allofus_male,
  concepts$gender_concepts$allofus_female
)
gender_results <- validate_concepts(con, gender_ids, "Gender")

# Validate gestational age concepts
cat("Validating gestational age observation concepts...\n")
gest_obs_results <- validate_concepts(
  con, 
  concepts$gestational_age_concepts$observation_concepts,
  "Gestational_Observation"
)

cat("Validating gestational age measurement concepts...\n")
gest_meas_results <- validate_concepts(
  con,
  concepts$gestational_age_concepts$measurement_concepts,
  "Gestational_Measurement"
)

# Validate pregnancy dating concepts
cat("Validating pregnancy dating concepts...\n")
delivery_date_results <- validate_concepts(
  con,
  concepts$pregnancy_dating$estimated_delivery_date_concepts,
  "Estimated_Delivery_Date"
)

conception_date_results <- validate_concepts(
  con,
  concepts$pregnancy_dating$estimated_conception_date_concepts,
  "Estimated_Conception_Date"
)

gest_length_results <- validate_concepts(
  con,
  concepts$pregnancy_dating$length_of_gestation_at_birth_concepts,
  "Gestation_Length"
)

# Combine all results
all_results <- bind_rows(
  gender_results,
  gest_obs_results,
  gest_meas_results,
  delivery_date_results,
  conception_date_results,
  gest_length_results
)

# Summary statistics
cat("\n=== VALIDATION SUMMARY ===\n")
summary_stats <- all_results %>%
  group_by(STATUS) %>%
  summarise(
    count = n(),
    percentage = round(100 * n() / nrow(all_results), 2)
  ) %>%
  arrange(desc(count))

print(summary_stats)

# Show invalid or missing concepts
invalid_missing <- all_results %>%
  filter(STATUS %in% c("MISSING", "INVALID", "NON_STANDARD"))

if (nrow(invalid_missing) > 0) {
  cat("\n=== CONCEPTS REQUIRING ATTENTION ===\n")
  print(invalid_missing %>% 
    select(INPUT_CONCEPT_ID, CONCEPT_TYPE, STATUS, CONCEPT_NAME, DOMAIN_ID))
  
  # Suggest alternatives for missing concepts
  cat("\n=== SEARCHING FOR ALTERNATIVE CONCEPTS ===\n")
  
  # Search for pregnancy-related concepts
  alt_sql <- "
  SELECT TOP 100
    concept_id,
    concept_name,
    domain_id,
    vocabulary_id,
    concept_class_id
  FROM concept
  WHERE standard_concept = 'S'
    AND invalid_reason IS NULL
    AND (
      LOWER(concept_name) LIKE '%pregnan%'
      OR LOWER(concept_name) LIKE '%gestation%'
      OR LOWER(concept_name) LIKE '%delivery%'
      OR LOWER(concept_name) LIKE '%birth%'
      OR LOWER(concept_name) LIKE '%abortion%'
      OR LOWER(concept_name) LIKE '%miscarriage%'
      OR LOWER(concept_name) LIKE '%ectopic%'
    )
    AND domain_id IN ('Condition', 'Observation', 'Procedure', 'Measurement')
  ORDER BY 
    CASE 
      WHEN vocabulary_id = 'SNOMED' THEN 1
      WHEN vocabulary_id = 'LOINC' THEN 2
      ELSE 3
    END,
    concept_name
  "
  
  alternatives <- querySql(con, alt_sql)
  cat("\nFound", nrow(alternatives), "alternative pregnancy-related concepts\n")
  
  # Save results
  write.csv(alternatives, "alternative_pregnancy_concepts.csv", row.names = FALSE)
  cat("Alternative concepts saved to: alternative_pregnancy_concepts.csv\n")
}

# Save validation results
write.csv(all_results, "concept_validation_results.csv", row.names = FALSE)
cat("\nValidation results saved to: concept_validation_results.csv\n")

# Check for required outcome concepts
cat("\n=== CHECKING PREGNANCY OUTCOME CONCEPTS ===\n")
outcome_sql <- "
SELECT 
  concept_id,
  concept_name,
  domain_id,
  vocabulary_id
FROM concept
WHERE concept_id IN (
  -- Live birth
  4092289, 4013886, 4012732,
  -- Stillbirth  
  4079978, 4013503, 435559,
  -- Spontaneous abortion
  4067106, 432303, 4066746,
  -- Induced abortion
  4128331, 4053936, 4236484,
  -- Ectopic pregnancy
  433260, 4098004, 198874,
  -- Delivery
  4081422, 4136974, 40485567
)
  AND standard_concept = 'S'
  AND invalid_reason IS NULL
"

outcome_results <- querySql(con, outcome_sql)
cat("Found", nrow(outcome_results), "valid outcome concepts\n")

if (nrow(outcome_results) < 15) {
  cat("WARNING: Some standard outcome concepts are missing. May need to map to local concepts.\n")
}

# Disconnect
disconnect(con)
cat("\nValidation complete!\n")