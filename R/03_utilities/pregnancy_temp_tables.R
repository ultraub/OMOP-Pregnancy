#' Pregnancy Cohort Temporary Table Management
#'
#' Helper functions for creating and managing temporary tables
#' specific to pregnancy cohort extraction.
#'
#' @import DatabaseConnector
#' @import SqlRender

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
    warning("No person IDs provided for temp table")
    return(NULL)
  }
  
  message(sprintf("  Creating person cohort temp table with %d persons...", 
                  length(person_ids)))
  
  # Create data frame with person IDs
  person_df <- data.frame(
    person_id = person_ids,
    stringsAsFactors = FALSE
  )
  
  # Get database type
  dbms <- attr(connection, "dbms")
  if (is.null(dbms)) {
    dbms <- "sql server"
  }
  
  # Drop table if exists
  if (dbms == "sql server") {
    drop_sql <- SqlRender::render(
      "IF OBJECT_ID('tempdb..@table_name') IS NOT NULL DROP TABLE @table_name",
      table_name = table_name
    )
    drop_sql <- SqlRender::translate(drop_sql, targetDialect = dbms)
    tryCatch(
      DatabaseConnector::executeSql(connection, drop_sql),
      error = function(e) {} # Ignore if doesn't exist
    )
  }
  
  # Create temp table using DatabaseConnector
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = table_name,
    data = person_df,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = TRUE,
    progressBar = FALSE
  )
  
  message(sprintf("  Created temp table %s", table_name))
  
  return(table_name)
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
  
  # Drop table if exists
  if (dbms == "sql server") {
    drop_sql <- SqlRender::render(
      "IF OBJECT_ID('tempdb..@table_name') IS NOT NULL DROP TABLE @table_name",
      table_name = table_name
    )
    drop_sql <- SqlRender::translate(drop_sql, targetDialect = dbms)
    tryCatch(
      DatabaseConnector::executeSql(connection, drop_sql),
      error = function(e) {} # Ignore if doesn't exist
    )
  }
  
  # Create temp table
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = table_name,
    data = concept_df,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = TRUE,
    progressBar = FALSE
  )
  
  message(sprintf("  Created temp table %s", table_name))
  
  return(table_name)
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
    # Default tables to clean up
    table_names <- c("#person_cohort", "#hip_concepts", "#pps_concepts", 
                    "#hip_conditions", "#hip_procedures", "#hip_observations",
                    "#hip_measurements")
  }
  
  message("  Cleaning up temporary tables...")
  
  dbms <- attr(connection, "dbms")
  if (is.null(dbms)) {
    dbms <- "sql server"
  }
  
  dropped <- 0
  failed <- 0
  
  for (table_name in table_names) {
    tryCatch({
      if (dbms == "sql server") {
        drop_sql <- SqlRender::render(
          "IF OBJECT_ID('tempdb..@table_name') IS NOT NULL DROP TABLE @table_name",
          table_name = table_name
        )
      } else {
        drop_sql <- SqlRender::render(
          "DROP TABLE IF EXISTS @table_name",
          table_name = table_name
        )
      }
      
      drop_sql <- SqlRender::translate(drop_sql, targetDialect = dbms)
      DatabaseConnector::executeSql(connection, drop_sql)
      dropped <- dropped + 1
    }, error = function(e) {
      failed <- failed + 1
    })
  }
  
  message(sprintf("  Dropped %d temp tables (%d failed or didn't exist)", 
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
      table_name <- paste0("#hip_", tolower(gsub(" ", "_", domain)))
      
      create_concept_temp_table(connection, domain_concepts, table_name)
      tables_created[[domain]] <- table_name
    }
  }
  
  return(tables_created)
}