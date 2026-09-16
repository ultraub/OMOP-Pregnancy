#' Extract Pregnancy Cohort from OMOP CDM
#'
#' Extracts all pregnancy-related data using temporary tables for efficiency.
#' This approach avoids huge WHERE IN clauses by using JOINs with temp tables,
#' making it scalable for large cohorts.
#' 
#' NOTE: DatabaseConnector automatically handles date conversion from different
#' database systems (SQL Server, PostgreSQL, Oracle, etc.). Dates are returned
#' as R Date objects, not numeric values. If dates appear as numeric, check
#' the database connection configuration.
#'
#' @param connection DatabaseConnector connection
#' @param cdm_schema Schema containing CDM tables
#' @param hip_concepts Data frame of HIP concepts
#' @param pps_concepts Data frame of PPS concepts
#' @param min_age Minimum age for inclusion
#' @param max_age Maximum age for inclusion
#' @param vocabulary_schema Schema containing the concept table (default: cdm_schema).
#'   Used to find ESD timing concepts by name, as the reference does.
#' @param male_concept_ids Gender concept IDs to exclude. Persons with any other
#'   gender_concept_id (including unknown) are eligible, matching the reference
#'   All of Us implementation which excludes only explicit males.
#'   Default is the standard OMOP MALE concept (8507).
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
  male_concept_ids = 8507,
  use_temp_tables = TRUE,
  vocabulary_schema = NULL
) {

  if (is.null(vocabulary_schema) || vocabulary_schema == "") {
    vocabulary_schema <- cdm_schema
  }
  
  
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
      # Create view directly from SQL - no data extraction to R!
      sql <- SqlRender::render("
        CREATE OR REPLACE TEMPORARY VIEW person_cohort AS
        SELECT DISTINCT person_id
        FROM @cdm_schema.person
        WHERE gender_concept_id NOT IN (@male_concept_ids)
          AND year_of_birth >= YEAR(CURRENT_DATE) - @max_age
          AND year_of_birth <= YEAR(CURRENT_DATE) - @min_age",
        cdm_schema = cdm_schema,
        male_concept_ids = male_concept_ids,
        min_age = min_age,
        max_age = max_age
      )
      
      sql <- SqlRender::translate(sql, targetDialect = target_dialect)
      db_execute(connection, sql)
      
      person_temp <- "person_cohort"
      temp_tables_created <- c(temp_tables_created, "person_cohort")
      
      # Still need persons data for demographics
      persons <- extract_persons(
        connection, cdm_schema, target_dialect,
        min_age, max_age, male_concept_ids
      )
      message(sprintf("  Created person cohort view with persons aged %d-%d", min_age, max_age))
      
    } else {
      # For SQL Server or when not using temp tables: Keep existing approach
      message("  Extracting person demographics...")
      persons <- extract_persons(
        connection, cdm_schema, target_dialect,
        min_age, max_age, male_concept_ids
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
      
      # Create concept temp tables
      message("  Creating concept temp tables...")

      # One HIP concept table joined to every domain table, as in the
      # reference. Records are found wherever the ETL placed them, and the
      # concept CSV needs no domain column.
      hip_temp <- NULL
      pps_temp <- NULL

      if (nrow(hip_concepts) > 0) {
        hip_temp <- create_concept_temp_table(connection, hip_concepts, "#hip_concepts")
        if (!is.null(hip_temp)) {
          temp_tables_created <- c(temp_tables_created, hip_temp)
        }
      }

      # PPS concepts
      if (nrow(pps_concepts) > 0) {
        pps_temp <- create_concept_temp_table(connection, pps_concepts, "#pps_concepts")
        if (!is.null(pps_temp)) {
          temp_tables_created <- c(temp_tables_created, pps_temp)
        }
      }

      # Extract using temp tables (use the actual returned names)
      message(sprintf("  Extracting conditions (%d concepts)...", nrow(hip_concepts)))
      conditions <- if (!is.null(hip_temp)) {
        extract_domain_with_temp_table(
          connection, cdm_schema, target_dialect,
          table_name = "condition_occurrence",
          concept_column = "condition_concept_id",
          date_column = "condition_start_date",
          person_temp_table = person_temp,
          concept_temp_table = hip_temp
        )
      } else {
        data.frame()
      }

      message(sprintf("  Extracting procedures (%d concepts)...", nrow(hip_concepts)))
      procedures <- if (!is.null(hip_temp)) {
        extract_domain_with_temp_table(
          connection, cdm_schema, target_dialect,
          table_name = "procedure_occurrence",
          concept_column = "procedure_concept_id",
          date_column = "procedure_date",
          person_temp_table = person_temp,
          concept_temp_table = hip_temp
        )
      } else {
        data.frame()
      }

      message(sprintf("  Extracting observations (%d concepts)...", nrow(hip_concepts)))
      observations <- if (!is.null(hip_temp)) {
        extract_domain_with_temp_table(
          connection, cdm_schema, target_dialect,
          table_name = "observation",
          concept_column = "observation_concept_id",
          date_column = "observation_date",
          person_temp_table = person_temp,
          concept_temp_table = hip_temp,
          include_value = TRUE
        )
      } else {
        data.frame()
      }

      message(sprintf("  Extracting measurements (%d concepts)...", nrow(hip_concepts)))
      measurements <- if (!is.null(hip_temp)) {
        extract_domain_with_temp_table(
          connection, cdm_schema, target_dialect,
          table_name = "measurement",
          concept_column = "measurement_concept_id",
          date_column = "measurement_date",
          person_temp_table = person_temp,
          concept_temp_table = hip_temp,
          include_value = TRUE
        )
      } else {
        data.frame()
      }
      
      # Extract gestational timing data
      message("  Extracting gestational timing data...")
      gestational_timing <- if (!is.null(pps_temp)) {
        extract_gestational_timing_with_temp_table(
          connection, cdm_schema, target_dialect,
          person_temp_table = person_temp,
          pps_temp_table = pps_temp
        )
      } else {
        data.frame()
      }

      # ESD timing evidence (concept-table driven, all four domain tables)
      message("  Extracting ESD timing records...")
      esd <- extract_esd_timing_records(
        connection, cdm_schema, vocabulary_schema, target_dialect,
        pps_concepts, person_temp_table = person_temp
      )
      esd_timing <- esd$records
      if (!is.null(esd$temp_table)) {
        temp_tables_created <- c(temp_tables_created, esd$temp_table)
      }
      
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
        concepts = hip_concepts,
        person_ids = person_ids
      )
      
      procedures <- extract_domain_table(
        connection, cdm_schema, target_dialect,
        table_name = "procedure_occurrence",
        concept_column = "procedure_concept_id",
        date_column = "procedure_date",
        concepts = hip_concepts,
        person_ids = person_ids
      )
      
      observations <- extract_domain_table(
        connection, cdm_schema, target_dialect,
        table_name = "observation",
        concept_column = "observation_concept_id",
        date_column = "observation_date",
        concepts = hip_concepts,
        person_ids = person_ids,
        include_value = TRUE
      )
      
      measurements <- extract_domain_table(
        connection, cdm_schema, target_dialect,
        table_name = "measurement",
        concept_column = "measurement_concept_id",
        date_column = "measurement_date",
        concepts = hip_concepts,
        person_ids = person_ids,
        include_value = TRUE
      )
      
      gestational_timing <- extract_gestational_timing(
        connection, cdm_schema, target_dialect,
        pps_concepts, person_ids
      )

      message("  Extracting ESD timing records...")
      esd <- extract_esd_timing_records(
        connection, cdm_schema, vocabulary_schema, target_dialect,
        pps_concepts, person_ids = person_ids
      )
      esd_timing <- esd$records
    }
    
    # Enforce types on all extracted data
    message("  Enforcing data types...")
    result <- list(
      persons = enforce_types(persons, "person"),
      conditions = enforce_types(conditions, "condition"),
      procedures = enforce_types(procedures, "procedure"),
      observations = enforce_types(observations, "observation"),
      measurements = enforce_types(measurements, "measurement"),
      gestational_timing = enforce_types(gestational_timing, "gestational"),
      esd_timing = enforce_types(esd_timing, "gestational")
    )

    # Keep only records where the person was of reproductive age at the event
    message("  Filtering records to age at event...")
    for (domain in c("conditions", "procedures", "observations",
                     "measurements", "gestational_timing", "esd_timing")) {
      result[[domain]] <- filter_records_by_age(
        result[[domain]], result$persons, min_age, max_age
      )
    }

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
  
  result <- db_query(connection, sql)
  
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

      UNION ALL

      -- Measurements (missing from original extraction)
      SELECT
        m.person_id,
        m.measurement_concept_id AS concept_id,
        m.measurement_date AS event_date,
        'Measurement' AS domain_name,
        m.value_as_number,
        NULL AS value_as_string,
        pc.min_month,
        pc.max_month
      FROM @cdm_schema.measurement m
      INNER JOIN @person_temp_table p ON m.person_id = p.person_id
      INNER JOIN @pps_temp_table pc ON m.measurement_concept_id = pc.concept_id

      UNION ALL

      -- Visit occurrence (matches original 5-table extraction)
      SELECT
        vo.person_id,
        vo.visit_concept_id AS concept_id,
        vo.visit_start_date AS event_date,
        'Visit' AS domain_name,
        NULL AS value_as_number,
        NULL AS value_as_string,
        pc.min_month,
        pc.max_month
      FROM @cdm_schema.visit_occurrence vo
      INNER JOIN @person_temp_table p ON vo.person_id = p.person_id
      INNER JOIN @pps_temp_table pc ON vo.visit_concept_id = pc.concept_id
    ) all_gestational
    ",
    cdm_schema = cdm_schema,
    person_temp_table = person_temp_table,
    pps_temp_table = pps_temp_table
  )
  
  sql <- SqlRender::translate(sql, targetDialect = target_dialect)
  
  result <- db_query(connection, sql)
  
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
  male_concept_ids = 8507
) {

  # Exclude explicit males only; everyone else is eligible (matches reference)
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
    WHERE p.gender_concept_id NOT IN (@male_concept_ids)
      AND p.year_of_birth >= YEAR(GETDATE()) - @max_age
      AND p.year_of_birth <= YEAR(GETDATE()) - @min_age
    ",
    cdm_schema = cdm_schema,
    male_concept_ids = male_concept_ids,
    min_age = min_age,
    max_age = max_age
  )
  
  sql <- SqlRender::translate(sql, targetDialect = target_dialect)
  
  result <- db_query(connection, sql)
  
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
    
    batch_result <- db_query(connection, sql)
    
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

        UNION ALL

        -- Measurements (25 of the 76 PPS concepts are LOINC labs)
        SELECT
          m.person_id,
          m.measurement_concept_id AS concept_id,
          m.measurement_date AS event_date,
          'Measurement' AS domain_name,
          m.value_as_number,
          NULL AS value_as_string
        FROM @cdm_schema.measurement m
        WHERE m.person_id IN (@person_ids)
          AND m.measurement_concept_id IN (@concept_ids)

        UNION ALL

        -- Visit occurrence (reference reads five tables)
        SELECT
          vo.person_id,
          vo.visit_concept_id AS concept_id,
          vo.visit_start_date AS event_date,
          'Visit' AS domain_name,
          NULL AS value_as_number,
          NULL AS value_as_string
        FROM @cdm_schema.visit_occurrence vo
        WHERE vo.person_id IN (@person_ids)
          AND vo.visit_concept_id IN (@concept_ids)
      ) all_gestational
      ",
      cdm_schema = cdm_schema,
      person_ids = batch_persons,
      concept_ids = pps_concept_ids
    )
    
    sql <- SqlRender::translate(sql, targetDialect = target_dialect)
    
    batch_result <- db_query(connection, sql)
    
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

#' Filter records to persons of reproductive age at the event date
#'
#' Matches the reference All of Us implementation: birth date is built from
#' year/month/day of birth with missing month or day imputed to 1, age is
#' (event_date - birth_date) / 365, and a record is kept when
#' min_age <= age < max_age.
#' @noRd
filter_records_by_age <- function(records, persons, min_age, max_age) {

  if (is.null(records) || nrow(records) == 0) {
    return(records)
  }

  birth_dates <- persons %>%
    transmute(
      person_id,
      birth_date = as.Date(sprintf(
        "%d-%02d-%02d",
        as.integer(year_of_birth),
        as.integer(coalesce(month_of_birth, 1L)),
        as.integer(coalesce(day_of_birth, 1L))
      ))
    )

  records %>%
    inner_join(birth_dates, by = "person_id") %>%
    mutate(age_at_event = as.numeric(as.Date(event_date) - birth_date) / 365) %>%
    filter(age_at_event >= min_age, age_at_event < max_age) %>%
    select(-birth_date, -age_at_event)
}

#' Extract ESD timing records
#'
#' Port of the reference get_timing_concepts() data step. Finds every concept
#' whose name contains "gestation period" plus the fixed ESD concept lists
#' and all PPS concepts in the vocabulary, then pulls matching records from
#' condition_occurrence, procedure_occurrence, observation and measurement
#' for the person cohort, carrying concept_name and the value columns.
#'
#' @return list(records = data frame, temp_table = name to drop or NULL)
#' @noRd
extract_esd_timing_records <- function(
  connection,
  cdm_schema,
  vocabulary_schema,
  target_dialect,
  pps_concepts,
  person_temp_table = NULL,
  person_ids = NULL
) {

  empty <- list(records = data.frame(), temp_table = NULL)

  fixed_ids <- unique(c(ESD_ALL_TIMING_CONCEPTS,
                        pps_concepts$concept_id[!is.na(pps_concepts$concept_id)]))

  concept_sql <- SqlRender::render("
    SELECT concept_id, concept_name
    FROM @vocabulary_schema.concept
    WHERE LOWER(concept_name) LIKE '%gestation period%'
       OR concept_id IN (@concept_ids)
    ",
    vocabulary_schema = vocabulary_schema,
    concept_ids = fixed_ids
  )
  concept_sql <- SqlRender::translate(concept_sql, targetDialect = target_dialect)
  esd_concepts <- db_query(connection, concept_sql)
  if (nrow(esd_concepts) == 0) {
    message("    No ESD timing concepts found in vocabulary")
    return(empty)
  }
  names(esd_concepts) <- tolower(names(esd_concepts))
  esd_concepts <- esd_concepts %>%
    mutate(concept_id = as.integer(concept_id), concept_name = as.character(concept_name)) %>%
    distinct(concept_id, .keep_all = TRUE)

  domain_select <- function(table, concept_col, date_col, domain, values) {
    sprintf("
      SELECT t.person_id, t.%s AS concept_id, t.%s AS event_date,
             '%s' AS domain_name, %s
      FROM @cdm_schema.%s t
      %%s", concept_col, date_col, domain, values, table)
  }
  selects <- c(
    domain_select("condition_occurrence", "condition_concept_id", "condition_start_date",
                  "Condition", "NULL AS value_as_number, NULL AS value_as_string"),
    domain_select("procedure_occurrence", "procedure_concept_id", "procedure_date",
                  "Procedure", "NULL AS value_as_number, NULL AS value_as_string"),
    domain_select("observation", "observation_concept_id", "observation_date",
                  "Observation", "t.value_as_number, t.value_as_string"),
    domain_select("measurement", "measurement_concept_id", "measurement_date",
                  "Measurement", "t.value_as_number, NULL AS value_as_string")
  )

  if (!is.null(person_temp_table)) {
    concept_temp <- create_concept_temp_table(connection, esd_concepts, "#esd_concepts")
    joins <- "INNER JOIN @person_temp_table p ON t.person_id = p.person_id
             INNER JOIN @concept_temp_table c ON t.concept_col = c.concept_id"
    parts <- mapply(function(sel, ccol) {
      sprintf(sel, gsub("concept_col", ccol, joins))
    }, selects, c("condition_concept_id", "procedure_concept_id",
                  "observation_concept_id", "measurement_concept_id"))
    sql <- SqlRender::render(
      paste0("SELECT * FROM (", paste(parts, collapse = " UNION ALL "), ") esd"),
      cdm_schema = cdm_schema,
      person_temp_table = person_temp_table,
      concept_temp_table = concept_temp
    )
    sql <- SqlRender::translate(sql, targetDialect = target_dialect)
    result <- db_query(connection, sql)
    if (nrow(result) > 0) names(result) <- tolower(names(result))
    result <- result %>% left_join(esd_concepts, by = "concept_id")
    return(list(records = result, temp_table = concept_temp))
  }

  if (is.null(person_ids) || length(person_ids) == 0) return(empty)

  batch_size <- 1000
  all_results <- list()
  for (i in seq(1, length(person_ids), by = batch_size)) {
    batch_persons <- person_ids[i:min(i + batch_size - 1, length(person_ids))]
    where <- "WHERE t.person_id IN (@person_ids) AND t.concept_col IN (@concept_ids)"
    parts <- mapply(function(sel, ccol) {
      sprintf(sel, gsub("concept_col", ccol, where))
    }, selects, c("condition_concept_id", "procedure_concept_id",
                  "observation_concept_id", "measurement_concept_id"))
    sql <- SqlRender::render(
      paste0("SELECT * FROM (", paste(parts, collapse = " UNION ALL "), ") esd"),
      cdm_schema = cdm_schema,
      person_ids = batch_persons,
      concept_ids = esd_concepts$concept_id
    )
    sql <- SqlRender::translate(sql, targetDialect = target_dialect)
    batch_result <- db_query(connection, sql)
    if (nrow(batch_result) > 0) {
      names(batch_result) <- tolower(names(batch_result))
      all_results[[length(all_results) + 1]] <- batch_result
    }
  }
  if (length(all_results) == 0) return(empty)
  result <- bind_rows(all_results) %>% left_join(esd_concepts, by = "concept_id")
  list(records = result, temp_table = NULL)
}
