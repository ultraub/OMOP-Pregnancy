#' Load HIP Concepts from CSV
#'
#' Loads HIP (Hierarchical Identification of Pregnancy) concepts
#' from the CSV file containing pregnancy-related concept definitions.
#'
#' @param file_path Path to HIP concepts CSV file (default: inst/concepts/HIP_concepts.csv or inst/csv/hip_concepts.csv)
#'
#' @return Data frame with columns:
#'   - concept_id: OMOP concept ID
#'   - concept_name: Human-readable concept name
#'   - domain: OMOP domain (Condition, Procedure, Observation, Measurement)
#'   - category: HIP category (LB, SB, DELIV, ECT, AB, SA, PREG, etc.)
#'   - gest_value: Gestational age value if applicable
#'
#' @export
load_hip_concepts <- function(file_path = NULL) {
  
  # Use default path if not provided
  if (is.null(file_path)) {
    # Try standard extdata directory
    file_path <- system.file("extdata", "hip_concepts.csv", package = "OMOPPregnancyV2")
    
    # Fallback to local development path if package not installed
    if (file_path == "") {
      if (file.exists("inst/extdata/hip_concepts.csv")) {
        file_path <- "inst/extdata/hip_concepts.csv"
      } else {
        stop("HIP concepts file not found. Please ensure package is properly installed.")
      }
    }
  }
  
  # Check file exists
  if (!file.exists(file_path)) {
    stop(sprintf("HIP concepts file not found at: %s", file_path))
  }
  
  # Read CSV
  concepts <- read.csv(file_path, stringsAsFactors = FALSE)
  
  # Add domain_name if missing (will be determined from concept table in database)
  # Using domain_name instead of domain to avoid SQL reserved keyword
  if (!"domain_name" %in% names(concepts)) {
    concepts$domain_name <- NA_character_
  }
  # Rename domain to domain_name if it exists
  if ("domain" %in% names(concepts)) {
    concepts <- concepts %>% rename(domain_name = domain)
  }
  
  # Validate required columns
  required_cols <- c("concept_id", "concept_name", "category")
  missing_cols <- setdiff(required_cols, names(concepts))
  if (length(missing_cols) > 0) {
    stop(sprintf("Missing required columns in HIP concepts: %s", 
                 paste(missing_cols, collapse = ", ")))
  }
  
  # Ensure correct data types
  concepts <- concepts %>%
    mutate(
      concept_id = as.integer(concept_id),
      concept_name = as.character(concept_name),
      domain_name = as.character(domain_name),
      category = as.character(category),
      gest_value = if ("gest_value" %in% names(.)) as.numeric(gest_value) else NA_real_
    )
  
  # Validate categories
  valid_categories <- c("LB", "SB", "DELIV", "ECT", "AB", "SA", "PREG", 
                       "GEST", "PCONF", "AMEN", "UP", "CONTRA")
  invalid_cats <- setdiff(unique(concepts$category), valid_categories)
  if (length(invalid_cats) > 0) {
    warning(sprintf("Unexpected categories in HIP concepts: %s",
                   paste(invalid_cats, collapse = ", ")))
  }
  
  # Remove duplicates
  concepts <- concepts %>%
    distinct(concept_id, .keep_all = TRUE)
  
  message(sprintf("Loaded %d HIP concepts across %d categories",
                  nrow(concepts), n_distinct(concepts$category)))
  
  return(concepts)
}

#' Load PPS Concepts from CSV
#'
#' Loads PPS (Pregnancy Progression Signatures) concepts with
#' gestational timing information.
#'
#' @param file_path Path to PPS concepts CSV file (default: inst/concepts/PPS_concepts.csv or inst/csv/pps_concepts.csv)
#'
#' @return Data frame with columns:
#'   - concept_id: OMOP concept ID
#'   - concept_name: Human-readable concept name
#'   - min_month: Minimum gestational month for this concept
#'   - max_month: Maximum gestational month for this concept
#'   - certainty: Confidence level (High, Medium, Low)
#'
#' @export
load_pps_concepts <- function(file_path = NULL) {
  
  # Use default path if not provided
  if (is.null(file_path)) {
    # Try standard extdata directory
    file_path <- system.file("extdata", "pps_concepts.csv", package = "OMOPPregnancyV2")
    
    # Fallback to local development path if package not installed
    if (file_path == "") {
      if (file.exists("inst/extdata/pps_concepts.csv")) {
        file_path <- "inst/extdata/pps_concepts.csv"
      } else {
        stop("PPS concepts file not found. Please ensure package is properly installed.")
      }
    }
  }
  
  # Check file exists
  if (!file.exists(file_path)) {
    stop(sprintf("PPS concepts file not found at: %s", file_path))
  }
  
  # Read CSV
  concepts <- read.csv(file_path, stringsAsFactors = FALSE)
  
  # Handle different column names from original format
  if ("domain_concept_id" %in% names(concepts)) {
    concepts$concept_id <- concepts$domain_concept_id
  }
  if ("domain_concept_name" %in% names(concepts)) {
    concepts$concept_name <- concepts$domain_concept_name
  }
  
  # Validate required columns
  required_cols <- c("concept_id", "concept_name", "min_month", "max_month")
  missing_cols <- setdiff(required_cols, names(concepts))
  if (length(missing_cols) > 0) {
    stop(sprintf("Missing required columns in PPS concepts: %s",
                 paste(missing_cols, collapse = ", ")))
  }
  
  # Ensure correct data types
  concepts <- concepts %>%
    mutate(
      concept_id = as.integer(concept_id),
      concept_name = as.character(concept_name),
      min_month = as.numeric(min_month),
      max_month = as.numeric(max_month),
      certainty = if ("certainty" %in% names(.)) as.character(certainty) else "Medium"
    )
  
  # Validate timing values
  invalid_timing <- concepts %>%
    filter(min_month < 0 | max_month > 12 | min_month > max_month)
  
  if (nrow(invalid_timing) > 0) {
    warning(sprintf("Found %d concepts with invalid timing values", nrow(invalid_timing)))
    # Remove invalid entries
    concepts <- concepts %>%
      filter(min_month >= 0, max_month <= 12, min_month <= max_month)
  }
  
  # Remove duplicates
  concepts <- concepts %>%
    distinct(concept_id, .keep_all = TRUE)
  
  message(sprintf("Loaded %d PPS concepts with gestational timing",
                  nrow(concepts)))
  
  return(concepts)
}

#' Load Matcho Term Limits
#'
#' Loads the Matcho term limits that define expected gestational
#' age ranges for different pregnancy outcomes.
#'
#' @param file_path Path to Matcho limits CSV file (default: inst/csv/matcho_limits.csv)
#'
#' @return Data frame with columns:
#'   - category: Outcome category (LB, SB, DELIV, etc.)
#'   - min_term: Minimum gestational days for this outcome
#'   - max_term: Maximum gestational days for this outcome
#'   - hierarchy: Priority order for overlapping outcomes
#'
#' @export
load_matcho_limits <- function(file_path = NULL) {
  
  # Use default path if not provided
  if (is.null(file_path)) {
    file_path <- system.file("extdata", "matcho_limits.csv", package = "OMOPPregnancyV2")
    
    # Fallback to local development path if package not installed
    if (file_path == "") {
      file_path <- "inst/extdata/matcho_limits.csv"
    }
  }
  
  # Check for file existence
  if (!file.exists(file_path)) {
    message("Matcho limits file not found, using defaults")
    return(get_default_matcho_limits())
  }
  
  # Read CSV
  limits <- read.csv(file_path, stringsAsFactors = FALSE)
  
  # Check if this is the outcome limits file (has min_days) or term durations file
  if ("min_days" %in% names(limits)) {
    # This is the outcome limits file with spacing between outcomes
    # Return as-is for HIP algorithm to use
    return(limits)
  }
  
  # Otherwise validate as term limits file
  required_cols <- c("category", "min_term", "max_term")
  missing_cols <- setdiff(required_cols, names(limits))
  if (length(missing_cols) > 0) {
    warning(sprintf("Missing columns in Matcho limits: %s. Using defaults.",
                   paste(missing_cols, collapse = ", ")))
    return(get_default_matcho_limits())
  }
  
  # Ensure correct data types
  limits <- limits %>%
    mutate(
      category = as.character(category),
      min_term = as.integer(min_term),
      max_term = as.integer(max_term),
      hierarchy = if ("hierarchy" %in% names(.)) as.integer(hierarchy) else row_number()
    ) %>%
    arrange(hierarchy)
  
  message(sprintf("Loaded Matcho limits for %d outcome categories", nrow(limits)))
  
  return(limits)
}

#' Get Default Matcho Limits
#'
#' Returns default Matcho term limits based on standard clinical definitions.
#'
#' @return Data frame with default term limits
#' @noRd
get_default_matcho_limits <- function() {
  data.frame(
    category = c("LB", "SB", "DELIV", "ECT", "AB", "SA", "PREG"),
    min_term = c(154, 140, 154, 30, 30, 30, 30),      # Days
    max_term = c(301, 301, 301, 84, 140, 140, 301),   # Days
    hierarchy = c(1, 2, 3, 4, 5, 6, 7),
    stringsAsFactors = FALSE
  )
}

#' Load Matcho Outcome Limits
#'
#' Loads the Matcho outcome limits that define minimum days
#' between different pregnancy outcome pairs.
#'
#' @param file_path Path to Matcho outcome limits CSV file
#'
#' @return Data frame with columns:
#'   - first_preg_category: First outcome category
#'   - outcome_preg_category: Second outcome category  
#'   - min_days: Minimum days required between outcomes
#'
#' @export
load_matcho_outcome_limits <- function(file_path = NULL) {
  
  # Use default path if not provided
  if (is.null(file_path)) {
    # Try standard extdata directory
    file_path <- system.file("extdata", "matcho_outcome_limits.csv", package = "OMOPPregnancyV2")
    
    # Fallback to local development path if package not installed
    if (file_path == "") {
      if (file.exists("inst/extdata/matcho_outcome_limits.csv")) {
        file_path <- "inst/extdata/matcho_outcome_limits.csv"
      } else {
        stop("Matcho outcome limits file not found. Please ensure package is properly installed.")
      }
    }
  }
  
  # Check file exists
  if (!file.exists(file_path)) {
    stop(sprintf("Matcho outcome limits file not found at: %s", file_path))
  }
  
  # Read CSV
  limits <- read.csv(file_path, stringsAsFactors = FALSE)
  
  # Validate required columns
  required_cols <- c("first_preg_category", "outcome_preg_category", "min_days")
  missing_cols <- setdiff(required_cols, names(limits))
  if (length(missing_cols) > 0) {
    stop(sprintf("Missing required columns in Matcho outcome limits: %s",
                 paste(missing_cols, collapse = ", ")))
  }
  
  # Ensure correct data types
  limits <- limits %>%
    mutate(
      first_preg_category = as.character(first_preg_category),
      outcome_preg_category = as.character(outcome_preg_category),
      min_days = as.integer(min_days)
    )
  
  message(sprintf("Loaded %d outcome spacing rules", nrow(limits)))
  
  return(limits)
}

#' Load All Pregnancy Concepts
#'
#' Convenience function to load all concept files needed for
#' pregnancy identification.
#'
#' @param hip_path Path to HIP concepts CSV
#' @param pps_path Path to PPS concepts CSV
#' @param matcho_path Path to Matcho limits CSV
#'
#' @return List containing:
#'   - hip_concepts: HIP concept definitions
#'   - pps_concepts: PPS concepts with timing
#'   - matcho_limits: Term limits for outcomes
#'
#' @export
load_pregnancy_concepts <- function(
  hip_path = NULL,
  pps_path = NULL,
  matcho_path = NULL
) {
  
  message("Loading pregnancy concept definitions...")
  
  # Load each concept set
  hip_concepts <- load_hip_concepts(hip_path)
  pps_concepts <- load_pps_concepts(pps_path)
  matcho_limits <- load_matcho_limits(matcho_path)
  
  # Create summary
  summary_stats <- list(
    n_hip_concepts = nrow(hip_concepts),
    n_pps_concepts = nrow(pps_concepts),
    hip_categories = unique(hip_concepts$category),
    hip_domains = unique(hip_concepts$domain_name),
    outcome_categories = unique(matcho_limits$category)
  )
  
  message(sprintf(
    "Loaded concepts: %d HIP, %d PPS, %d outcome categories",
    summary_stats$n_hip_concepts,
    summary_stats$n_pps_concepts,
    length(summary_stats$outcome_categories)
  ))
  
  return(list(
    hip_concepts = hip_concepts,
    pps_concepts = pps_concepts,
    matcho_limits = matcho_limits,
    summary = summary_stats
  ))
}

#' Validate Concept Coverage
#'
#' Checks that concepts cover expected domains and categories.
#'
#' @param hip_concepts HIP concepts data frame
#' @param pps_concepts PPS concepts data frame
#'
#' @return List of validation results and warnings
#' @export
validate_concept_coverage <- function(hip_concepts, pps_concepts) {
  
  validation_results <- list()
  
  # Check HIP coverage
  expected_hip_categories <- c("LB", "SB", "DELIV", "ECT", "AB", "SA", "PREG")
  missing_hip <- setdiff(expected_hip_categories, unique(hip_concepts$category))
  if (length(missing_hip) > 0) {
    validation_results$missing_hip_categories <- missing_hip
    warning(sprintf("Missing HIP categories: %s", paste(missing_hip, collapse = ", ")))
  }
  
  # Check domain coverage
  expected_domains <- c("Condition", "Procedure", "Observation", "Measurement")
  missing_domains <- setdiff(expected_domains, unique(hip_concepts$domain_name))
  if (length(missing_domains) > 0) {
    validation_results$missing_domains <- missing_domains
    warning(sprintf("Missing domains: %s", paste(missing_domains, collapse = ", ")))
  }
  
  # Check PPS timing coverage
  pps_coverage <- pps_concepts %>%
    summarise(
      covers_early = any(min_month <= 3),
      covers_mid = any(min_month <= 6 & max_month >= 6),
      covers_late = any(max_month >= 9)
    )
  
  if (!all(unlist(pps_coverage))) {
    validation_results$incomplete_pps_coverage <- pps_coverage
    warning("PPS concepts do not cover all pregnancy stages")
  }
  
  # Check for concept overlap
  overlap_concepts <- intersect(hip_concepts$concept_id, pps_concepts$concept_id)
  if (length(overlap_concepts) > 0) {
    validation_results$overlapping_concepts <- overlap_concepts
    message(sprintf("Found %d concepts in both HIP and PPS", length(overlap_concepts)))
  }
  
  validation_results$is_valid <- length(validation_results) == 1  # Only overlap is OK
  
  return(validation_results)
}

#' Enrich Concepts with Domain Information
#'
#' Queries the OMOP concept table to add domain_id information
#' to concept definitions.
#'
#' @param concepts Data frame with concept_id column
#' @param connection DatabaseConnector connection
#' @param vocabulary_schema Schema containing concept table
#'
#' @return Data frame with domain column added
#' @export
enrich_concepts_with_domains <- function(concepts, connection, vocabulary_schema) {
  
  if (is.null(connection)) {
    warning("No database connection provided, cannot enrich with domains")
    return(concepts)
  }
  
  # Get unique concept IDs
  concept_ids <- unique(concepts$concept_id)
  
  # Query concept table for domains
  sql <- SqlRender::render("
    SELECT 
      concept_id,
      domain_id
    FROM @vocabulary_schema.concept
    WHERE concept_id IN (@concept_ids)
  ",
    vocabulary_schema = vocabulary_schema,
    concept_ids = paste(concept_ids, collapse = ",")
  )
  
  sql <- SqlRender::translate(
    sql, 
    targetDialect = attr(connection, "dbms")
  )
  
  domain_mapping <- DatabaseConnector::querySql(connection, sql)
  names(domain_mapping) <- tolower(names(domain_mapping))
  
  # Merge domains back to concepts
  # Rename domain_id to avoid SQL reserved keyword warning
  if ("domain_id" %in% names(domain_mapping)) {
    domain_mapping <- domain_mapping %>%
      rename(domain_id_value = domain_id)
  }
  
  concepts_enriched <- concepts %>%
    left_join(
      domain_mapping,
      by = "concept_id"
    ) %>%
    mutate(
      # Use existing domain_name if available, otherwise use from database
      # Using domain_name to avoid SQL reserved keyword
      domain_name = coalesce(domain_name, domain_id_value),
      # Map common domain names
      domain_name = case_when(
        domain_name == "Condition" ~ "Condition",
        domain_name == "Procedure" ~ "Procedure", 
        domain_name == "Observation" ~ "Observation",
        domain_name == "Measurement" ~ "Measurement",
        domain_name == "Drug" ~ "Drug",
        domain_name == "Device" ~ "Device",
        TRUE ~ domain_name
      )
    ) %>%
    select(-domain_id_value)  # Remove temporary column
  
  # Report missing domains
  missing_domains <- concepts_enriched %>%
    filter(is.na(domain_name)) %>%
    select(concept_id, concept_name)
  
  if (nrow(missing_domains) > 0) {
    warning(sprintf(
      "Could not find domains for %d concepts. May be non-standard or invalid concepts.",
      nrow(missing_domains)
    ))
  }
  
  return(concepts_enriched)
}

#' Load All Concept Sets
#'
#' Wrapper function that loads all required concept sets for the HIPPS algorithm.
#' This function coordinates loading of HIP concepts, PPS concepts, and all
#' supporting lookup tables.
#'
#' @param hip_path Path to HIP concepts CSV file (optional)
#' @param pps_path Path to PPS concepts CSV file (optional) 
#' @param matcho_path Path to Matcho limits CSV file (optional)
#' @param outcome_limits_path Path to outcome limits CSV file (optional)
#'
#' @return List containing:
#'   - hip_concepts: HIP concept data frame
#'   - pps_concepts: PPS concept data frame  
#'   - matcho_limits: Matcho category limits
#'   - matcho_outcome_limits: Outcome-specific limits
#'   - matcho_term_durations: Term duration mappings
#'
#' @export
load_concept_sets <- function(
  hip_path = NULL,
  pps_path = NULL,
  matcho_path = NULL,
  outcome_limits_path = NULL
) {
  
  message("Loading concept sets...")
  
  # Load each concept set
  hip_concepts <- load_hip_concepts(hip_path)
  pps_concepts <- load_pps_concepts(pps_path)
  matcho_limits <- load_matcho_limits(matcho_path)
  matcho_outcome_limits <- load_matcho_outcome_limits(outcome_limits_path)
  
  # Load term durations if available
  term_durations_path <- system.file("extdata", "matcho_term_durations.csv", package = "OMOPPregnancyV2")
  if (term_durations_path == "") {
    if (file.exists("inst/extdata/matcho_term_durations.csv")) {
      term_durations_path <- "inst/extdata/matcho_term_durations.csv"
    }
  }
  
  if (file.exists(term_durations_path)) {
    matcho_term_durations <- readr::read_csv(term_durations_path, col_types = readr::cols())
  } else {
    # Default term durations
    matcho_term_durations <- data.frame(
      outcome = c("LB", "SB", "DELIV", "ECT", "AB", "SA", "PREG"),
      term_days = c(280, 280, 280, 63, 91, 91, 280),
      stringsAsFactors = FALSE
    )
  }
  
  message(sprintf("  - Loaded %d HIP concepts", nrow(hip_concepts)))
  message(sprintf("  - Loaded %d PPS concepts", nrow(pps_concepts)))
  message(sprintf("  - Loaded %d outcome categories", nrow(matcho_limits)))
  
  return(list(
    hip_concepts = hip_concepts,
    pps_concepts = pps_concepts,
    matcho_limits = matcho_limits,
    matcho_outcome_limits = matcho_outcome_limits,
    matcho_term_durations = matcho_term_durations
  ))
}