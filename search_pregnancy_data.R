# Comprehensive search for pregnancy data in Tufts database
suppressPackageStartupMessages({
  library(DBI)
  library(odbc)
  library(dplyr)
  library(dbplyr)
})

cat("========================================\n")
cat("Comprehensive Pregnancy Data Search\n")
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

# Search for pregnancy-related conditions
cat("--- Searching Condition Occurrences ---\n")
condition_tbl <- tbl(con, "condition_occurrence")

# Broader pregnancy condition concepts
pregnancy_conditions <- c(
  # Pregnancy conditions
  433260, 4088927, 4154377, 4299535, # General pregnancy
  # Delivery
  4013024, 4145193, 4047364, 4129218, 4092289,
  # Complications
  433824, 439393, 4046090, 4054251,
  # Prenatal
  4184637, 4083487, 4295136
)

preg_conditions <- condition_tbl %>%
  filter(condition_concept_id %in% pregnancy_conditions) %>%
  group_by(condition_concept_id) %>%
  count() %>%
  collect()

if(nrow(preg_conditions) > 0) {
  cat("Found pregnancy conditions:\n")
  print(preg_conditions)
} else {
  cat("No pregnancy conditions found with standard concepts\n")
}

# Search for pregnancy-related observations
cat("\n--- Searching Observations ---\n")
observation_tbl <- tbl(con, "observation")

# Pregnancy observation concepts
pregnancy_obs <- c(
  # Pregnancy status
  4053609, 4184451, 4296029,
  # Prenatal visits
  4005823, 4263689,
  # Pregnancy test
  4041273, 4299360, 4024962
)

preg_obs <- observation_tbl %>%
  filter(observation_concept_id %in% pregnancy_obs) %>%
  group_by(observation_concept_id) %>%
  count() %>%
  collect()

if(nrow(preg_obs) > 0) {
  cat("Found pregnancy observations:\n")
  print(preg_obs)
} else {
  cat("No pregnancy observations found with standard concepts\n")
}

# Search for ANY pregnancy-related text in source values
cat("\n--- Searching for pregnancy keywords in source values ---\n")

# Check measurements with pregnancy keywords
measurement_tbl <- tbl(con, "measurement")
preg_measurements <- measurement_tbl %>%
  filter(
    measurement_source_value %like% "%pregn%" |
    measurement_source_value %like% "%gestat%" |
    measurement_source_value %like% "%delivery%" |
    measurement_source_value %like% "%labor%" |
    measurement_source_value %like% "%fetal%" |
    measurement_source_value %like% "%antenatal%" |
    measurement_source_value %like% "%prenatal%"
  ) %>%
  head(100) %>%
  collect()

cat("Pregnancy-related measurements found:", nrow(preg_measurements), "\n")
if(nrow(preg_measurements) > 0) {
  cat("Sample measurement source values:\n")
  unique_vals <- unique(preg_measurements$measurement_source_value)[1:min(10, length(unique(preg_measurements$measurement_source_value)))]
  for(val in unique_vals) {
    cat("  -", val, "\n")
  }
}

# Check conditions with pregnancy keywords
preg_conditions_text <- condition_tbl %>%
  filter(
    condition_source_value %like% "%pregn%" |
    condition_source_value %like% "%gestat%" |
    condition_source_value %like% "%delivery%" |
    condition_source_value %like% "%labor%"
  ) %>%
  head(100) %>%
  collect()

cat("\nPregnancy-related conditions (by text) found:", nrow(preg_conditions_text), "\n")
if(nrow(preg_conditions_text) > 0) {
  cat("Sample condition source values:\n")
  unique_vals <- unique(preg_conditions_text$condition_source_value)[1:min(10, length(unique(preg_conditions_text$condition_source_value)))]
  for(val in unique_vals) {
    cat("  -", val, "\n")
  }
}

# Check procedures with pregnancy keywords  
procedure_tbl <- tbl(con, "procedure_occurrence")
preg_procedures_text <- procedure_tbl %>%
  filter(
    procedure_source_value %like% "%pregn%" |
    procedure_source_value %like% "%delivery%" |
    procedure_source_value %like% "%cesarean%" |
    procedure_source_value %like% "%natal%"
  ) %>%
  head(100) %>%
  collect()

cat("\nPregnancy-related procedures (by text) found:", nrow(preg_procedures_text), "\n")
if(nrow(preg_procedures_text) > 0) {
  cat("Sample procedure source values:\n")
  unique_vals <- unique(preg_procedures_text$procedure_source_value)[1:min(10, length(unique(preg_procedures_text$procedure_source_value)))]
  for(val in unique_vals) {
    cat("  -", val, "\n")
  }
}

# Check observations with pregnancy keywords
preg_obs_text <- observation_tbl %>%
  filter(
    observation_source_value %like% "%pregn%" |
    observation_source_value %like% "%gravid%" |
    observation_source_value %like% "%parity%"
  ) %>%
  head(100) %>%
  collect()

cat("\nPregnancy-related observations (by text) found:", nrow(preg_obs_text), "\n")
if(nrow(preg_obs_text) > 0) {
  cat("Sample observation source values:\n")
  unique_vals <- unique(preg_obs_text$observation_source_value)[1:min(10, length(unique(preg_obs_text$observation_source_value)))]
  for(val in unique_vals) {
    cat("  -", val, "\n")
  }
}

# Get sample of women with any visits
cat("\n--- Sample Women with Healthcare Encounters ---\n")
person_tbl <- tbl(con, "person")
visit_tbl <- tbl(con, "visit_occurrence")

women_with_visits <- person_tbl %>%
  filter(gender_concept_id == 8532) %>%
  inner_join(visit_tbl, by = "person_id") %>%
  group_by(person_id) %>%
  summarise(visit_count = n()) %>%
  arrange(desc(visit_count)) %>%
  head(10) %>%
  collect()

cat("Top 10 women by visit count:\n")
print(women_with_visits)

# Check age distribution of women
women_ages <- person_tbl %>%
  filter(gender_concept_id == 8532) %>%
  mutate(birth_year = year_of_birth) %>%
  mutate(age_2024 = 2024 - birth_year) %>%
  filter(age_2024 >= 15 & age_2024 <= 50) %>%  # Reproductive age
  count() %>%
  collect()

cat("\nWomen of reproductive age (15-50 in 2024):", women_ages$n, "\n")

dbDisconnect(con)
cat("\n✓ Search complete\n")