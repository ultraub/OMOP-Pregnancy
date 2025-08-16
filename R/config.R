#' Configuration Management for OMOP Pregnancy Package
#'
#' Functions to manage configuration settings, concept sets, and algorithm parameters
#'
#' @name config
#' @import readr
#' @import readxl
#' @import yaml
NULL

#' Load package configuration
#'
#' Loads configuration settings for the HIPPS algorithm
#'
#' @param config_file Path to configuration file (optional)
#' @param mode Character string: "generic" or "allofus"
#' @param concepts_config Path to OMOP concepts configuration file (optional)
#'
#' @return List with configuration settings
#' @export
load_config <- function(config_file = NULL, mode = "generic", concepts_config = NULL) {
  
  # Default configuration
  config <- list(
    mode = mode,
    
    # Algorithm parameters
    algorithm = list(
      hip = list(
        min_age = 15,
        max_age = 55,
        outcome_hierarchy = c("LB", "SB", "ECT", "AB", "SA", "DELIV")
      ),
      pps = list(
        min_records = 2,
        max_gap_months = 10,
        tolerance_months = 2
      ),
      esd = list(
        use_term_limits = TRUE
      )
    ),
    
    # Episode gaps (days)
    episode_gaps = list(
      same_outcome = 168,
      different_outcome = 112,
      abortion = 112,
      ectopic = 56
    ),
    
    # Gestational limits (weeks)
    gestational_limits = list(
      min_weeks = 4,
      max_weeks = 44,
      livebirth_min = 20,
      abortion_max = 19
    ),
    
    # Database settings
    database = list(
      batch_size = 10000,
      use_temp_tables = TRUE,
      parallel = FALSE
    ),
    
    # Output settings
    output = list(
      save_intermediate = TRUE,
      compress_output = TRUE,
      min_cell_size = 11
    )
  )
  
  # Load custom configuration if provided
  if (!is.null(config_file)) {
    if (file.exists(config_file)) {
      custom_config <- yaml::read_yaml(config_file)
      # Merge custom config with defaults
      config <- modifyList(config, custom_config)
    } else {
      warning(paste("Configuration file not found:", config_file))
    }
  }
  
  # Load OMOP concepts configuration
  if (is.null(concepts_config)) {
    # Try to load from package installation
    concepts_config <- system.file("config", "omop_concepts.yaml", package = "OMOPPregnancy")
  }
  
  if (file.exists(concepts_config)) {
    concepts <- yaml::read_yaml(concepts_config)
    config$concepts <- concepts
    
    # Merge algorithm parameters from concepts config
    if (!is.null(concepts$algorithm_parameters)) {
      config$algorithm$hip$min_age <- concepts$algorithm_parameters$min_age
      config$algorithm$hip$max_age <- concepts$algorithm_parameters$max_age
      config$gestational_limits$min_weeks <- concepts$algorithm_parameters$min_gestational_weeks
      config$gestational_limits$max_weeks <- concepts$algorithm_parameters$max_gestational_weeks
    }
    
    # Apply database-specific settings
    if (!is.null(config$database$platform)) {
      platform <- tolower(config$database$platform)
      if (!is.null(concepts$database_settings$batch_sizes[[platform]])) {
        config$database$batch_size <- concepts$database_settings$batch_sizes[[platform]]
      }
      # Store date functions for the dialect
      if (!is.null(concepts$database_settings$date_diff_functions[[platform]])) {
        config$database$date_diff_function <- concepts$database_settings$date_diff_functions[[platform]]
      }
      if (!is.null(concepts$database_settings$date_add_days[[platform]])) {
        config$database$date_add_function <- concepts$database_settings$date_add_days[[platform]]
      }
    }
  } else {
    warning("OMOP concepts configuration file not found. Using hardcoded default values.")
    config$concepts <- get_default_concepts()
  }
  
  # Mode-specific adjustments
  if (mode == "allofus") {
    config$database$platform <- "bigquery"
    config$database$use_aou_functions <- TRUE
    # Use All of Us specific concept IDs for gender
    if (!is.null(config$concepts$gender_concepts)) {
      config$concepts$gender_concepts$active_male_id <- config$concepts$gender_concepts$allofus_male
      config$concepts$gender_concepts$active_female_id <- config$concepts$gender_concepts$allofus_female
    }
  } else {
    # Use standard OMOP concept IDs for gender
    if (!is.null(config$concepts$gender_concepts)) {
      config$concepts$gender_concepts$active_male_id <- config$concepts$gender_concepts$male_concept_id
      config$concepts$gender_concepts$active_female_id <- config$concepts$gender_concepts$female_concept_id
    }
  }
  
  return(config)
}

#' Get default OMOP concepts
#'
#' Returns default concept IDs when configuration file is not available
#'
#' @return List with default concept configurations
#' @keywords internal
get_default_concepts <- function() {
  list(
    gender_concepts = list(
      male_concept_id = 8507,
      female_concept_id = 8532,
      allofus_male = 45880669,
      allofus_female = 45878463,
      active_male_id = 8507,
      active_female_id = 8532
    ),
    gestational_age_concepts = list(
      gestational_age = 3012266,
      gestational_age_estimated = 3002209,
      gestational_age_in_weeks = 3048230,
      observation_concepts = c(3011536, 3026070, 3024261, 4260747, 40758410, 3002549, 
                               43054890, 46234792, 4266763, 40485048, 3048230, 3002209, 3012266),
      measurement_concepts = c(3036844, 3048230, 3001105, 3002209, 3050433, 3012266)
    ),
    pregnancy_dating = list(
      estimated_delivery_date_concepts = c(1175623, 3001105, 3011536, 3024261, 3024973, 
                                          3026070, 3036322, 3038318, 3038608, 4059478, 
                                          4128833, 40490322, 40760182, 40760183, 42537958),
      estimated_conception_date_concepts = c(3002314, 3043737, 4058439, 4072438, 4089559, 44817092),
      length_of_gestation_at_birth_concepts = c(4260747, 43054890, 46234792, 4266763, 40485048)
    ),
    database_settings = list(
      batch_sizes = list(
        postgresql = 50000,
        sql_server = 25000,
        oracle = 10000,
        bigquery = 50000,
        default = 10000
      ),
      page_sizes = list(
        large_query = 50000,
        medium_query = 20000,
        small_query = 10000
      )
    ),
    algorithm_parameters = list(
      min_age = 15,
      max_age = 56,
      min_gestational_weeks = 0,
      max_gestational_weeks = 44,
      invalid_gestational_value = 9999999
    )
  )
}

#' Validate configuration settings
#'
#' Checks that configuration settings are valid and consistent
#'
#' @param config Configuration list
#'
#' @return Logical TRUE if valid, error otherwise
#' @export
validate_config <- function(config) {
  
  # Check required elements exist
  required_elements <- c("mode", "algorithm", "episode_gaps", "gestational_limits")
  missing_elements <- setdiff(required_elements, names(config))
  
  if (length(missing_elements) > 0) {
    stop(paste("Missing required configuration elements:",
               paste(missing_elements, collapse = ", ")))
  }
  
  # Validate age ranges
  if (config$algorithm$hip$min_age >= config$algorithm$hip$max_age) {
    stop("Invalid age range: min_age must be less than max_age")
  }
  
  # Validate gestational limits
  if (config$gestational_limits$min_weeks >= config$gestational_limits$max_weeks) {
    stop("Invalid gestational limits: min_weeks must be less than max_weeks")
  }
  
  # Validate outcome hierarchy
  valid_outcomes <- c("LB", "SB", "ECT", "AB", "SA", "DELIV", "PREG")
  invalid_outcomes <- setdiff(config$algorithm$hip$outcome_hierarchy, valid_outcomes)
  
  if (length(invalid_outcomes) > 0) {
    stop(paste("Invalid outcomes in hierarchy:",
               paste(invalid_outcomes, collapse = ", ")))
  }
  
  message("Configuration validation successful")
  return(TRUE)
}

#' Load concept sets from files
#'
#' Loads the concept sets required for the HIPPS algorithm
#'
#' @param concept_dir Directory containing concept files
#' @param format Format of concept files: "excel" or "csv"
#'
#' @return List containing concept data frames
#' @export
load_concept_sets <- function(concept_dir = NULL, format = "auto") {
  
  # Use default directory if not specified
  if (is.null(concept_dir)) {
    concept_dir <- system.file("extdata", package = "OMOPPregnancy")
  }
  
  # Check directory exists
  if (!dir.exists(concept_dir)) {
    stop(paste("Concept directory not found:", concept_dir))
  }
  
  concept_sets <- list()
  
  # Auto-detect format: prefer CSV if available, fallback to Excel
  if (format == "auto") {
    csv_test <- file.path(concept_dir, "HIP_concepts.csv")
    if (file.exists(csv_test)) {
      format <- "csv"
    } else {
      format <- "excel"
    }
  }
  
  if (format == "excel") {
    # Load Excel files
    hip_file <- file.path(concept_dir, "HIP_concepts.xlsx")
    pps_file <- file.path(concept_dir, "PPS_concepts.xlsx")
    matcho_limits_file <- file.path(concept_dir, "Matcho_outcome_limits.xlsx")
    matcho_terms_file <- file.path(concept_dir, "Matcho_term_durations.xlsx")
    
    if (file.exists(hip_file)) {
      concept_sets$HIP_concepts <- readxl::read_excel(hip_file)
    }
    if (file.exists(pps_file)) {
      concept_sets$PPS_concepts <- readxl::read_excel(pps_file) %>%
        dplyr::mutate(domain_concept_id = as.integer(domain_concept_id))
    }
    if (file.exists(matcho_limits_file)) {
      concept_sets$Matcho_outcome_limits <- readxl::read_excel(matcho_limits_file)
    }
    if (file.exists(matcho_terms_file)) {
      concept_sets$Matcho_term_durations <- readxl::read_excel(matcho_terms_file)
    }
    
  } else if (format == "csv") {
    # Load CSV files
    hip_file <- file.path(concept_dir, "HIP_concepts.csv")
    pps_file <- file.path(concept_dir, "PPS_concepts.csv")
    matcho_limits_file <- file.path(concept_dir, "Matcho_outcome_limits.csv")
    matcho_terms_file <- file.path(concept_dir, "Matcho_term_durations.csv")
    
    if (file.exists(hip_file)) {
      concept_sets$HIP_concepts <- readr::read_csv(hip_file, show_col_types = FALSE)
      # Fix NA handling for Databricks - convert "NA" strings to proper R NA values
      # The CSV has "NA" as strings which need to be proper NULL/NA for database upload
      if ("gest_value" %in% names(concept_sets$HIP_concepts)) {
        # Ensure gest_value is numeric and NA strings become proper NA
        concept_sets$HIP_concepts$gest_value <- as.numeric(concept_sets$HIP_concepts$gest_value)
      }
    }
    if (file.exists(pps_file)) {
      concept_sets$PPS_concepts <- readr::read_csv(pps_file, show_col_types = FALSE) %>%
        dplyr::mutate(domain_concept_id = as.integer(domain_concept_id))
    }
    if (file.exists(matcho_limits_file)) {
      concept_sets$Matcho_outcome_limits <- readr::read_csv(matcho_limits_file, show_col_types = FALSE)
    }
    if (file.exists(matcho_terms_file)) {
      concept_sets$Matcho_term_durations <- readr::read_csv(matcho_terms_file, show_col_types = FALSE)
    }
  }
  
  # Validate concept sets
  required_sets <- c("HIP_concepts", "PPS_concepts", 
                    "Matcho_outcome_limits", "Matcho_term_durations")
  missing_sets <- setdiff(required_sets, names(concept_sets))
  
  if (length(missing_sets) > 0) {
    stop(paste("Missing required concept sets:",
               paste(missing_sets, collapse = ", ")))
  }
  
  message(paste("Loaded", length(concept_sets), "concept sets"))
  return(concept_sets)
}

#' Configure algorithm parameters
#'
#' Sets up algorithm-specific parameters based on configuration
#'
#' @param config Configuration list
#' @param cdm_version CDM version string
#'
#' @return Updated configuration list
#' @export
configure_algorithm <- function(config, cdm_version = "5.3") {
  
  # CDM version-specific adjustments
  if (cdm_version == "5.4" || cdm_version == "6.0") {
    # CDM 5.4+ has different datetime handling
    config$database$use_datetime <- TRUE
    message("Configuring for CDM version", cdm_version)
  } else {
    config$database$use_datetime <- FALSE
  }
  
  # Platform-specific optimizations
  if (!is.null(config$database$platform)) {
    platform <- tolower(config$database$platform)
    
    if (platform == "bigquery") {
      # BigQuery optimizations
      config$database$batch_size <- 50000
      config$database$use_temp_tables <- TRUE
      
    } else if (platform == "redshift") {
      # Redshift optimizations
      config$database$batch_size <- 25000
      config$database$use_distkey <- TRUE
      
    } else if (platform == "sql server" || platform == "pdw") {
      # SQL Server optimizations
      config$database$batch_size <- 10000
      config$database$use_columnstore <- TRUE
      
    } else if (platform == "oracle") {
      # Oracle optimizations
      config$database$batch_size <- 10000
      config$database$use_parallel <- TRUE
    }
  }
  
  return(config)
}

#' Save configuration to file
#'
#' Exports current configuration settings to a YAML file
#'
#' @param config Configuration list
#' @param file_path Path to save configuration file
#'
#' @return NULL
#' @export
save_config <- function(config, file_path) {
  
  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("Package 'yaml' required for saving configuration")
  }
  
  yaml::write_yaml(config, file_path)
  message(paste("Configuration saved to:", file_path))
  return(invisible(NULL))
}

#' Get default concept set mappings
#'
#' Returns the standard concept ID to category mappings
#'
#' @return Data frame with concept mappings
#' @export
get_concept_mappings <- function() {
  
  # Standard outcome concept categories
  mappings <- data.frame(
    category = c("LB", "SB", "ECT", "AB", "SA", "DELIV", "PREG"),
    description = c(
      "Live Birth",
      "Stillbirth",
      "Ectopic Pregnancy",
      "Induced Abortion",
      "Spontaneous Abortion",
      "Delivery (unspecified)",
      "Pregnancy (no outcome)"
    ),
    priority = c(1, 2, 3, 4, 4, 5, 6),
    min_gestational_weeks = c(20, 20, 4, 4, 4, 20, 4),
    max_gestational_weeks = c(44, 44, 13, 19, 19, 44, 44),
    stringsAsFactors = FALSE
  )
  
  return(mappings)
}
