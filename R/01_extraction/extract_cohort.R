#' Extract Pregnancy Cohort from OMOP CDM
#'
#' Extracts all pregnancy-related data using temporary tables for efficiency.
#' This approach avoids huge WHERE IN clauses by using JOINs with temp tables,
#' making it scalable for large cohorts.
#'
#' @param connection DatabaseConnector connection
#' @param cdm_schema Schema containing CDM tables
#' @param hip_concepts Data frame of HIP concepts
#' @param pps_concepts Data frame of PPS concepts
#' @param min_age Minimum age for inclusion
#' @param max_age Maximum age for inclusion
#' @param use_temp_tables Whether to use temp tables (default: TRUE)
#'
#' @return List containing extracted cohort data
#' @export
extract_pregnancy_cohort <- function(
  connection,
  cdm_schema,
  hip_concepts,
  pps_concepts,
  min_age = 15,
  max_age = 56,
  use_all_of_us_gender = FALSE,
  use_temp_tables = TRUE
) {
  
  # Source helper functions
  source("R/03_utilities/pregnancy_temp_tables.R")
  
  # Get database type for SqlRender
  target_dialect <- attr(connection, "dbms")
  if (is.null(target_dialect)) {
    stop("Database type (dbms) not set on connection")
  }
  
  # Track temp tables for cleanup
  temp_tables_created <- c()
  
  tryCatch({
    
    message("  Creating person cohort view...")
    
    # For Databricks/Spark: Create view directly without extracting to R
    if (target_dialect %in% c("spark", "databricks") && use_temp_tables) {
      # Gender concept IDs
      if (use_all_of_us_gender) {
        gender_concepts <- c(45878463, 45880669, 903096, 903079, 1177221)
      } else {
        gender_concepts <- c(8532, 8507, 45878463)  # Female, FEMALE, Woman
      }
      
      # Create view directly from SQL - no data extraction to R!
      sql <- SqlRender::render("
        CREATE OR REPLACE TEMPORARY VIEW person_cohort AS
        SELECT DISTINCT person_id
        FROM @cdm_schema.person
        WHERE gender_concept_id IN (@gender_concepts)
          AND year_of_birth >= YEAR(CURRENT_DATE) - @max_age
          AND year_of_birth <= YEAR(CURRENT_DATE) - @min_age",
        cdm_schema = cdm_schema,
        gender_concepts = gender_concepts,
        min_age = min_age,
        max_age = max_age
      )
      
      sql <- SqlRender::translate(sql, targetDialect = target_dialect)
      DatabaseConnector::executeSql(connection, sql)
      
      person_temp <- "person_cohort"
      temp_tables_created <- c(temp_tables_created, "person_cohort")
      
      # Still need persons data for demographics
      persons <- extract_persons(
        connection, cdm_schema, target_dialect,
        min_age, max_age, use_all_of_us_gender
      )
      message(sprintf("  Created person cohort view with persons aged %d-%d", min_age, max_age))
      
    } else {
      # For SQL Server or when not using temp tables: Keep existing approach
      message("  Extracting person demographics...")
      persons <- extract_persons(
        connection, cdm_schema, target_dialect,
        min_age, max_age, use_all_of_us_gender
      )
      
      # Get person IDs for filtering
      person_ids <- unique(persons$person_id)
      message(sprintf("  Found %d persons", length(person_ids)))
      
      if (use_temp_tables && length(person_ids) > 0) {
        # Create person cohort temp table
        person_temp <- create_person_temp_table(connection, person_ids)
        # Store the base name for cleanup tracking
        temp_tables_created <- c(temp_tables_created, "person_cohort")
      }
    }
    
    if (use_temp_tables && exists("person_temp")) {
      
      # Create concept temp tables by domain
      message("  Creating concept temp tables...")
      
      # HIP concepts by domain
      hip_conditions <- hip_concepts %>% 
        filter(!is.na(domain_name) & domain_name == "Condition")
      if (nrow(hip_conditions) > 0) {
        hip_cond_temp <- create_concept_temp_table(connection, hip_conditions, "#hip_conditions")
        temp_tables_created <- c(temp_tables_created, hip_cond_temp)
      }
      
      hip_procedures <- hip_concepts %>% 
        filter(!is.na(domain_name) & domain_name == "Procedure")
      if (nrow(hip_procedures) > 0) {
        hip_proc_temp <- create_concept_temp_table(connection, hip_procedures, "#hip_procedures")
        temp_tables_created <- c(temp_tables_created, hip_proc_temp)
      }
      
      hip_observations <- hip_concepts %>% 
        filter(!is.na(domain_name) & domain_name == "Observation")
      if (nrow(hip_observations) > 0) {
        hip_obs_temp <- create_concept_temp_table(connection, hip_observations, "#hip_observations")
        temp_tables_created <- c(temp_tables_created, hip_obs_temp)
      }
      
      hip_measurements <- hip_concepts %>% 
        filter(!is.na(domain_name) & domain_name == "Measurement")
      if (nrow(hip_measurements) > 0) {
        hip_meas_temp <- create_concept_temp_table(connection, hip_measurements, "#hip_measurements")
        temp_tables_created <- c(temp_tables_created, hip_meas_temp)
      }
      
      # PPS concepts
      if (nrow(pps_concepts) > 0) {
        pps_temp <- create_concept_temp_table(connection, pps_concepts, "#pps_concepts")
        temp_tables_created <- c(temp_tables_created, pps_temp)
      }
      
      # Extract using temp tables
      message(sprintf("  Extracting conditions (%d concepts)...", nrow(hip_conditions)))
      conditions <- extract_domain_with_temp_table(
        connection, cdm_schema, target_dialect,
        table_name = "condition_occurrence",
        concept_column = "condition_concept_id",
        date_column = "condition_start_date",
        person_temp_table = person_temp,
        concept_temp_table = "#hip_conditions"
      )
      
      message(sprintf("  Extracting procedures (%d concepts)...", nrow(hip_procedures)))
      procedures <- extract_domain_with_temp_table(
        connection, cdm_schema, target_dialect,
        table_name = "procedure_occurrence",
        concept_column = "procedure_concept_id",
        date_column = "procedure_date",
        person_temp_table = person_temp,
        concept_temp_table = "#hip_procedures"
      )
      
      message(sprintf("  Extracting observations (%d concepts)...", nrow(hip_observations)))
      observations <- extract_domain_with_temp_table(
        connection, cdm_schema, target_dialect,
        table_name = "observation",
        concept_column = "observation_concept_id",
        date_column = "observation_date",
        person_temp_table = person_temp,
        concept_temp_table = "#hip_observations",
        include_value = TRUE
      )
      
      message(sprintf("  Extracting measurements (%d concepts)...", nrow(hip_measurements)))
      measurements <- extract_domain_with_temp_table(
        connection, cdm_schema, target_dialect,
        table_name = "measurement",
        concept_column = "measurement_concept_id",
        date_column = "measurement_date",
        person_temp_table = person_temp,
        concept_temp_table = "#hip_measurements",
        include_value = TRUE
      )
      
      # Extract gestational timing data
      message("  Extracting gestational timing data...")
      gestational_timing <- extract_gestational_timing_with_temp_table(
        connection, cdm_schema, target_dialect,
        person_temp_table = person_temp,
        pps_temp_table = "#pps_concepts"
      )
      
    } else {
      # Fall back to original method for small cohorts
      message("  Using direct extraction (small cohort or temp tables disabled)...")
      
      # Make sure we have person_ids for non-temp table approach
      if (!exists("person_ids")) {
        person_ids <- unique(persons$person_id)
      }
      
      conditions <- extract_domain_table(
        connection, cdm_schema, target_dialect,
        table_name = "condition_occurrence",
        concept_column = "condition_concept_id",
        date_column = "condition_start_date",
        concepts = hip_concepts[!is.na(hip_concepts$domain_name) & hip_concepts$domain_name == "Condition", ],
        person_ids = person_ids
      )
      
      procedures <- extract_domain_table(
        connection, cdm_schema, target_dialect,
        table_name = "procedure_occurrence",
        concept_column = "procedure_concept_id",
        date_column = "procedure_date",
        concepts = hip_concepts[!is.na(hip_concepts$domain_name) & hip_concepts$domain_name == "Procedure", ],
        person_ids = person_ids
      )
      
      observations <- extract_domain_table(
        connection, cdm_schema, target_dialect,
        table_name = "observation",
        concept_column = "observation_concept_id",
        date_column = "observation_date",
        concepts = hip_concepts[!is.na(hip_concepts$domain_name) & hip_concepts$domain_name == "Observation", ],
        person_ids = person_ids,
        include_value = TRUE
      )
      
      measurements <- extract_domain_table(
        connection, cdm_schema, target_dialect,
        table_name = "measurement",
        concept_column = "measurement_concept_id",
        date_column = "measurement_date",
        concepts = hip_concepts[!is.na(hip_concepts$domain_name) & hip_concepts$domain_name == "Measurement", ],
        person_ids = person_ids,
        include_value = TRUE
      )
      
      gestational_timing <- extract_gestational_timing(
        connection, cdm_schema, target_dialect,
        pps_concepts, person_ids
      )
    }
    
    # Enforce types on all extracted data
    message("  Enforcing data types...")
    result <- list(
      persons = enforce_types(persons, "person"),
      conditions = enforce_types(conditions, "condition"),
      procedures = enforce_types(procedures, "procedure"),
      observations = enforce_types(observations, "observation"),
      measurements = enforce_types(measurements, "measurement"),
      gestational_timing = enforce_types(gestational_timing, "gestational")
    )
    
    return(result)
    
  }, finally = {
    # Always cleanup temp tables
    if (length(temp_tables_created) > 0) {
      cleanup_pregnancy_temp_tables(connection, temp_tables_created)
    }
  })
}

#' Extract Domain Table Using Temp Tables
#' @noRd
extract_domain_with_temp_table <- function(
  connection,
  cdm_schema,
  target_dialect,
  table_name,
  concept_column,
  date_column,
  person_temp_table,
  concept_temp_table,
  include_value = FALSE
) {
  
  if (is.null(concept_temp_table)) {
    message(sprintf("    No concepts for %s", table_name))
    return(data.frame())
  }
  
  # Build value columns if needed
  # Note: measurement table has value_as_concept_id instead of value_as_string
  value_select <- if (include_value) {
    if (table_name == "measurement") {
      ", t.value_as_number, CAST(t.value_as_concept_id AS VARCHAR(50)) AS value_as_string"
    } else {
      ", t.value_as_number, t.value_as_string"
    }
  } else {
    ", NULL AS value_as_number, NULL AS value_as_string"
  }
  
  # Create SQL using JOINs with temp tables
  sql <- SqlRender::render(paste0("
    SELECT
      t.person_id,
      t.", concept_column, " AS concept_id,
      t.", date_column, " AS event_date,
      c.concept_name,
      c.category,
      c.gest_value",
      value_select, "
    FROM @cdm_schema.", table_name, " t
    INNER JOIN @person_temp_table p ON t.person_id = p.person_id
    INNER JOIN @concept_temp_table c ON t.", concept_column, " = c.concept_id
    WHERE t.", date_column, " IS NOT NULL
    "),
    cdm_schema = cdm_schema,
    person_temp_table = person_temp_table,
    concept_temp_table = concept_temp_table
  )
  
  sql <- SqlRender::translate(sql, targetDialect = target_dialect)
  
  result <- DatabaseConnector::querySql(connection, sql)
  
  # Convert column names to lowercase for consistency
  if (nrow(result) > 0) {
    names(result) <- tolower(names(result))
  }
  
  return(result)
}

#' Extract Gestational Timing Using Temp Tables
#' @noRd
extract_gestational_timing_with_temp_table <- function(
  connection,
  cdm_schema,
  target_dialect,
  person_temp_table,
  pps_temp_table
) {
  
  if (is.null(pps_temp_table)) {
    message("    No PPS concepts for gestational timing")
    return(data.frame())
  }
  
  # Get all tables that might contain gestational timing
  sql <- SqlRender::render("
    SELECT 
      person_id,
      concept_id,
      event_date,
      domain_name,
      value_as_number,
      value_as_string,
      min_month,
      max_month
    FROM (
      -- Conditions
      SELECT 
        co.person_id,
        co.condition_concept_id AS concept_id,
        co.condition_start_date AS event_date,
        'Condition' AS domain_name,
        NULL AS value_as_number,
        NULL AS value_as_string,
        pc.min_month,
        pc.max_month
      FROM @cdm_schema.condition_occurrence co
      INNER JOIN @person_temp_table p ON co.person_id = p.person_id
      INNER JOIN @pps_temp_table pc ON co.condition_concept_id = pc.concept_id
      
      UNION ALL
      
      -- Procedures
      SELECT 
        po.person_id,
        po.procedure_concept_id AS concept_id,
        po.procedure_date AS event_date,
        'Procedure' AS domain_name,
        NULL AS value_as_number,
        NULL AS value_as_string,
        pc.min_month,
        pc.max_month
      FROM @cdm_schema.procedure_occurrence po
      INNER JOIN @person_temp_table p ON po.person_id = p.person_id
      INNER JOIN @pps_temp_table pc ON po.procedure_concept_id = pc.concept_id
      
      UNION ALL
      
      -- Observations
      SELECT 
        o.person_id,
        o.observation_concept_id AS concept_id,
        o.observation_date AS event_date,
        'Observation' AS domain_name,
        o.value_as_number,
        o.value_as_string,
        pc.min_month,
        pc.max_month
      FROM @cdm_schema.observation o
      INNER JOIN @person_temp_table p ON o.person_id = p.person_id
      INNER JOIN @pps_temp_table pc ON o.observation_concept_id = pc.concept_id
    ) all_gestational
    ",
    cdm_schema = cdm_schema,
    person_temp_table = person_temp_table,
    pps_temp_table = pps_temp_table
  )
  
  sql <- SqlRender::translate(sql, targetDialect = target_dialect)
  
  result <- DatabaseConnector::querySql(connection, sql)
  
  # Convert column names to lowercase for consistency
  if (nrow(result) > 0) {
    names(result) <- tolower(names(result))
  }
  
  return(result)
}

#' Extract Persons from OMOP CDM
#' @noRd
extract_persons <- function(
  connection,
  cdm_schema,
  target_dialect,
  min_age,
  max_age,
  use_all_of_us_gender = FALSE
) {
  
  # Gender concept IDs
  if (use_all_of_us_gender) {
    # All of Us gender concepts
    gender_concepts <- c(45878463, 45880669, 903096, 903079, 1177221)
  } else {
    # Standard OMOP female concepts
    gender_concepts <- c(8532, 8507, 45878463)  # Female, FEMALE, Woman
  }
  
  # Build SQL for person extraction
  sql <- SqlRender::render("
    SELECT DISTINCT
      p.person_id,
      p.gender_concept_id,
      p.year_of_birth,
      p.month_of_birth,
      p.day_of_birth,
      p.race_concept_id,
      p.ethnicity_concept_id,
      YEAR(GETDATE()) - p.year_of_birth AS age_current
    FROM @cdm_schema.person p
    WHERE p.gender_concept_id IN (@gender_concepts)
      AND p.year_of_birth >= YEAR(GETDATE()) - @max_age
      AND p.year_of_birth <= YEAR(GETDATE()) - @min_age
    ",
    cdm_schema = cdm_schema,
    gender_concepts = gender_concepts,
    min_age = min_age,
    max_age = max_age
  )
  
  sql <- SqlRender::translate(sql, targetDialect = target_dialect)
  
  result <- DatabaseConnector::querySql(connection, sql)
  
  # Convert column names to lowercase
  if (nrow(result) > 0) {
    names(result) <- tolower(names(result))
  }
  
  return(result)
}

#' Extract Domain Table (for non-temp table approach)
#' @noRd
extract_domain_table <- function(
  connection,
  cdm_schema,
  target_dialect,
  table_name,
  concept_column,
  date_column,
  concepts,
  person_ids,
  include_value = FALSE
) {
  
  if (is.null(concepts) || nrow(concepts) == 0 || length(person_ids) == 0) {
    return(data.frame())
  }
  
  # For small cohorts, we can use WHERE IN
  # This is the fallback when not using temp tables
  if (length(person_ids) > 10000) {
    warning(sprintf("Large cohort (%d persons) without temp tables may be slow", length(person_ids)))
  }
  
  concept_ids <- unique(concepts$concept_id[!is.na(concepts$concept_id)])
  
  if (length(concept_ids) == 0) {
    return(data.frame())
  }
  
  # Build value columns if needed
  value_select <- if (include_value) {
    if (table_name == "measurement") {
      ", t.value_as_number, CAST(t.value_as_concept_id AS VARCHAR(50)) AS value_as_string"
    } else {
      ", t.value_as_number, t.value_as_string"
    }
  } else {
    ", NULL AS value_as_number, NULL AS value_as_string"
  }
  
  # Process in batches to avoid SQL length limits
  batch_size <- 1000
  all_results <- list()
  
  for (i in seq(1, length(person_ids), by = batch_size)) {
    batch_persons <- person_ids[i:min(i + batch_size - 1, length(person_ids))]
    
    sql <- SqlRender::render(paste0("
      SELECT
        t.person_id,
        t.", concept_column, " AS concept_id,
        t.", date_column, " AS event_date",
        value_select, "
      FROM @cdm_schema.", table_name, " t
      WHERE t.person_id IN (@person_ids)
        AND t.", concept_column, " IN (@concept_ids)
        AND t.", date_column, " IS NOT NULL
    "),
      cdm_schema = cdm_schema,
      person_ids = batch_persons,
      concept_ids = concept_ids
    )
    
    sql <- SqlRender::translate(sql, targetDialect = target_dialect)
    
    batch_result <- DatabaseConnector::querySql(connection, sql)
    
    if (nrow(batch_result) > 0) {
      names(batch_result) <- tolower(names(batch_result))
      
      # Add concept metadata
      batch_result <- batch_result %>%
        left_join(
          concepts %>% select(concept_id, concept_name, category, gest_value),
          by = "concept_id"
        )
      
      all_results[[length(all_results) + 1]] <- batch_result
    }
  }
  
  if (length(all_results) > 0) {
    return(bind_rows(all_results))
  } else {
    return(data.frame())
  }
}

#' Extract Gestational Timing (for non-temp table approach)
#' @noRd
extract_gestational_timing <- function(
  connection,
  cdm_schema,
  target_dialect,
  pps_concepts,
  person_ids
) {
  
  if (is.null(pps_concepts) || nrow(pps_concepts) == 0 || length(person_ids) == 0) {
    return(data.frame())
  }
  
  pps_concept_ids <- unique(pps_concepts$concept_id[!is.na(pps_concepts$concept_id)])
  
  if (length(pps_concept_ids) == 0) {
    return(data.frame())
  }
  
  # Process in batches
  batch_size <- 1000
  all_results <- list()
  
  for (i in seq(1, length(person_ids), by = batch_size)) {
    batch_persons <- person_ids[i:min(i + batch_size - 1, length(person_ids))]
    
    sql <- SqlRender::render("
      SELECT 
        person_id,
        concept_id,
        event_date,
        domain_name,
        value_as_number,
        value_as_string
      FROM (
        -- Conditions
        SELECT 
          co.person_id,
          co.condition_concept_id AS concept_id,
          co.condition_start_date AS event_date,
          'Condition' AS domain_name,
          NULL AS value_as_number,
          NULL AS value_as_string
        FROM @cdm_schema.condition_occurrence co
        WHERE co.person_id IN (@person_ids)
          AND co.condition_concept_id IN (@concept_ids)
        
        UNION ALL
        
        -- Procedures
        SELECT 
          po.person_id,
          po.procedure_concept_id AS concept_id,
          po.procedure_date AS event_date,
          'Procedure' AS domain_name,
          NULL AS value_as_number,
          NULL AS value_as_string
        FROM @cdm_schema.procedure_occurrence po
        WHERE po.person_id IN (@person_ids)
          AND po.procedure_concept_id IN (@concept_ids)
        
        UNION ALL
        
        -- Observations
        SELECT 
          o.person_id,
          o.observation_concept_id AS concept_id,
          o.observation_date AS event_date,
          'Observation' AS domain_name,
          o.value_as_number,
          o.value_as_string
        FROM @cdm_schema.observation o
        WHERE o.person_id IN (@person_ids)
          AND o.observation_concept_id IN (@concept_ids)
      ) all_gestational
      ",
      cdm_schema = cdm_schema,
      person_ids = batch_persons,
      concept_ids = pps_concept_ids
    )
    
    sql <- SqlRender::translate(sql, targetDialect = target_dialect)
    
    batch_result <- DatabaseConnector::querySql(connection, sql)
    
    if (nrow(batch_result) > 0) {
      names(batch_result) <- tolower(names(batch_result))
      
      # Add PPS timing metadata
      batch_result <- batch_result %>%
        left_join(
          pps_concepts %>% select(concept_id, min_month, max_month),
          by = "concept_id"
        )
      
      all_results[[length(all_results) + 1]] <- batch_result
    }
  }
  
  if (length(all_results) > 0) {
    return(bind_rows(all_results))
  } else {
    return(data.frame())
  }
}
