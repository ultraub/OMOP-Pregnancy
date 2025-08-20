#' Pregnancy Cohort Temporary Table Management
#'
#' Helper functions for creating and managing temporary tables
#' specific to pregnancy cohort extraction.
#'
#' @import DatabaseConnector
#' @import SqlRender
#' @import DBI

#' Create Person Cohort Temporary Table
#'
#' Creates a temporary table containing person IDs for efficient joining.
#'
#' @param connection DatabaseConnector connection
#' @param person_ids Vector of person IDs
#' @param table_name Name for temp table (default: "#person_cohort")
#'
#' @return Name of the created temp table
#' @export
create_person_temp_table <- function(connection, person_ids, table_name = "#person_cohort") {
  
  if (length(person_ids) == 0) {
    warning("No person IDs provided")
    return(NULL)
  }
  
  dbms <- attr(connection, "dbms")
  if (is.null(dbms)) {
    dbms <- "sql server"
  }
  
  message(sprintf("  Creating person cohort with %d persons...", length(person_ids)))
  
  # For Databricks/Spark: Use temporary view (instant, no data movement)
  if (dbms %in% c("spark", "databricks")) {
    view_name <- gsub("^#", "", table_name)
    
    # Create view with VALUES clause - much faster than inserting data
    # Split into chunks if needed to avoid SQL length limits
    max_ids_per_query <- 10000
    
    if (length(person_ids) <= max_ids_per_query) {
      sql <- sprintf(
        "CREATE OR REPLACE TEMPORARY VIEW %s AS SELECT * FROM (VALUES %s) AS t(person_id)",
        view_name,
        paste0("(", person_ids, ")", collapse = ",")
      )
      DatabaseConnector::executeSql(connection, sql)
    } else {
      # For very large lists, create a temp table instead
      # But do it efficiently with CREATE TABLE AS SELECT
      results_schema <- attr(connection, "results_schema")
      full_table_name <- get_full_table_name(connection, view_name, schema = results_schema, dbms = "spark")
      
      # Create table with first batch
      first_batch <- person_ids[1:max_ids_per_query]
      sql <- sprintf(
        "CREATE OR REPLACE TABLE %s AS SELECT * FROM (VALUES %s) AS t(person_id)",
        full_table_name,
        paste0("(", first_batch, ")", collapse = ",")
      )
      DatabaseConnector::executeSql(connection, sql)
      
      # Insert remaining in larger batches using INSERT INTO
      for (i in seq(max_ids_per_query + 1, length(person_ids), by = max_ids_per_query)) {
        batch_end <- min(i + max_ids_per_query - 1, length(person_ids))
        batch <- person_ids[i:batch_end]
        
        sql <- sprintf(
          "INSERT INTO %s SELECT * FROM (VALUES %s) AS t(person_id)",
          full_table_name,
          paste0("(", batch, ")", collapse = ",")
        )
        DatabaseConnector::executeSql(connection, sql)
      }
      view_name <- full_table_name
    }
    
    message(sprintf("  Created %s", view_name))
    # Return the view name as it will be used in queries
    # For temporary views, just the base name without schema
    # For tables, the full qualified name
    if (length(person_ids) <= max_ids_per_query) {
      return(view_name)  # Just the base name for temporary view
    } else {
      return(full_table_name)  # Full qualified name for table
    }
    
  } else {
    # SQL Server: Keep using temp tables (they're already fast)
    if (!grepl("^#", table_name)) {
      table_name <- paste0("#", table_name)
    }
    
    person_df <- data.frame(
      person_id = person_ids,
      stringsAsFactors = FALSE
    )
    
    temp_table <- ohdsi_create_temp_table(
      connection = connection,
      data = person_df,
      table_name = table_name,
      overwrite = TRUE
    )
    
    message(sprintf("  Created temp table %s", table_name))
    return(if (inherits(temp_table, "tbl")) table_name else temp_table)
  }
}

#' Create Concept Temporary Table
#'
#' Creates a temporary table containing concept definitions with metadata.
#'
#' @param connection DatabaseConnector connection
#' @param concepts Data frame of concepts with concept_id, concept_name, category, etc.
#' @param table_name Name for temp table
#'
#' @return Name of the created temp table
#' @export
create_concept_temp_table <- function(connection, concepts, table_name) {
  
  if (nrow(concepts) == 0) {
    warning(sprintf("No concepts provided for temp table %s", table_name))
    return(NULL)
  }
  
  message(sprintf("  Creating concept temp table %s with %d concepts...", 
                  table_name, nrow(concepts)))
  
  # Prepare concepts data frame (use domain_name to avoid SQL reserved keyword)
  concept_df <- concepts %>%
    select(any_of(c("concept_id", "concept_name", "domain_name", "category", 
                    "gest_value", "min_month", "max_month", "certainty")))
  
  # Ensure required columns exist
  if (!"concept_id" %in% names(concept_df)) {
    stop("concept_id column is required")
  }
  
  # Get database type
  dbms <- attr(connection, "dbms")
  if (is.null(dbms)) {
    dbms <- "sql server"
  }
  
  # Get results schema for Spark/Databricks
  results_schema <- attr(connection, "results_schema")
  
  # Adjust table name based on platform
  if (dbms == "sql server" && !grepl("^#", table_name)) {
    table_name <- paste0("#", table_name)
  } else if (dbms %in% c("spark", "databricks")) {
    # Remove # prefix for Spark
    table_name <- gsub("^#", "", table_name)
  }
  
  # Use the unified temp table creation function
  temp_table <- ohdsi_create_temp_table(
    connection = connection,
    data = concept_df,
    table_name = table_name,
    resultsDatabaseSchema = results_schema,
    overwrite = TRUE
  )
  
  message(sprintf("  Created temp table %s", table_name))
  
  # Return the table name for backward compatibility
  if (inherits(temp_table, "tbl")) {
    return(table_name)
  } else {
    return(temp_table)
  }
}

#' Cleanup Pregnancy Cohort Temporary Tables
#'
#' Drops all temporary tables created during pregnancy cohort extraction.
#'
#' @param connection DatabaseConnector connection
#' @param table_names Vector of temp table names to drop
#'
#' @export
cleanup_pregnancy_temp_tables <- function(connection, table_names = NULL) {
  
  if (is.null(table_names)) {
    # Default tables to clean up (without # prefix - will be added as needed)
    table_names <- c("person_cohort", "hip_concepts", "pps_concepts", 
                    "hip_conditions", "hip_procedures", "hip_observations",
                    "hip_measurements")
  }
  
  message("  Cleaning up temporary tables...")
  
  dbms <- attr(connection, "dbms")
  if (is.null(dbms)) {
    dbms <- "sql server"
  }
  
  # Get results schema for Spark/Databricks
  results_schema <- attr(connection, "results_schema")
  
  dropped <- 0
  failed <- 0
  
  for (table_name in table_names) {
    tryCatch({
      # Adjust table name based on platform
      if (dbms == "sql server") {
        # Add # prefix if not present
        if (!grepl("^#", table_name)) {
          table_name <- paste0("#", table_name)
        }
        drop_sql <- SqlRender::render(
          "IF OBJECT_ID('tempdb..@table_name') IS NOT NULL DROP TABLE @table_name",
          table_name = table_name
        )
      } else if (dbms %in% c("spark", "databricks")) {
        # Remove # prefix for Spark
        clean_table_name <- gsub("^#", "", table_name)
        
        # For person_cohort, it's created as a TEMPORARY VIEW without schema
        # For other tables, they might have schema qualification
        if (clean_table_name == "person_cohort") {
          # person_cohort is created as a temporary view without schema prefix
          drop_view_sql <- SqlRender::render(
            "DROP VIEW IF EXISTS @table_name",
            table_name = clean_table_name
          )
          
          tryCatch(
            {
              DatabaseConnector::executeSql(connection, drop_view_sql)
              dropped <- dropped + 1
            },
            error = function(e) {
              # Silently ignore - view might not exist
            }
          )
        } else {
          # Other tables might have schema qualification
          if (!is.null(results_schema) && !grepl("\\.", clean_table_name)) {
            # Add schema prefix if not already present
            full_table_name <- paste(results_schema, clean_table_name, sep = ".")
          } else {
            full_table_name <- clean_table_name
          }
          
          # Try dropping as view first
          drop_view_sql <- SqlRender::render(
            "DROP VIEW IF EXISTS @table_name",
            table_name = full_table_name
          )
          
          tryCatch(
            {
              DatabaseConnector::executeSql(connection, drop_view_sql)
              dropped <- dropped + 1
            },
            error = function(e) {
              # Silently ignore - view might not exist
            }
          )
          
          # Also try dropping as table (for uploaded data)
          drop_table_sql <- SqlRender::render(
            "DROP TABLE IF EXISTS @table_name",
            table_name = full_table_name
          )
          
          tryCatch(
            {
              DatabaseConnector::executeSql(connection, drop_table_sql)
              dropped <- dropped + 1
            },
            error = function(e) {
              # Silently ignore - table might not exist
            }
          )
        }
        
        next  # Skip the final execute at the end
      } else {
        # Generic SQL for other platforms
        drop_sql <- SqlRender::render(
          "DROP TABLE IF EXISTS @table_name",
          table_name = table_name
        )
      }
      
      # Execute for non-Spark platforms
      if (!dbms %in% c("spark", "databricks")) {
        drop_sql <- SqlRender::translate(drop_sql, targetDialect = dbms)
        DatabaseConnector::executeSql(connection, drop_sql)
        dropped <- dropped + 1
      }
    }, error = function(e) {
      # For Databricks, "table not found" errors are expected and should be silent
      if (dbms %in% c("spark", "databricks") && 
          grepl("(TABLE_OR_VIEW_NOT_FOUND|cannot be found|does not exist)", e$message, ignore.case = TRUE)) {
        # This is expected - table was already dropped or never existed
        # Don't increment failed counter or log
      } else {
        failed <- failed + 1
        # Only log unexpected errors in interactive mode
        if (interactive()) {
          message(sprintf("    Warning: Unexpected error dropping %s: %s", table_name, e$message))
        }
      }
    })
  }
  
  message(sprintf("  Dropped %d temp tables/views (%d failed or didn't exist)", 
                  dropped, failed))
  
  invisible(NULL)
}

#' Create Domain-Specific Concept Tables
#'
#' Creates separate temp tables for each domain to optimize queries.
#'
#' @param connection DatabaseConnector connection
#' @param hip_concepts HIP concepts data frame
#'
#' @return List of created temp table names
#' @export
create_domain_concept_tables <- function(connection, hip_concepts) {
  
  tables_created <- list()
  
  # Create tables for each domain
  domains <- unique(hip_concepts$domain_name[!is.na(hip_concepts$domain_name)])
  
  for (domain in domains) {
    domain_concepts <- hip_concepts %>%
      filter(!is.na(domain_name) & domain_name == !!domain)
    
    if (nrow(domain_concepts) > 0) {
      # Create table name without # prefix (will be added if needed)
      table_name <- paste0("hip_", tolower(gsub(" ", "_", domain)))
      
      create_concept_temp_table(connection, domain_concepts, table_name)
      tables_created[[domain]] <- table_name
    }
  }
  
  return(tables_created)
}