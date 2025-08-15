#' Query Utilities for OMOP CDM
#'
#' Functions to handle database queries and temporary tables across different
#' OMOP CDM platforms, replacing All of Us-specific functions.
#'
#' @name query_utils
#' @import dplyr
#' @import DBI
NULL

# Source the SQL functions module for cross-platform support
# This provides SqlRender-based wrappers for date and string operations

#' Create a temporary table from a data frame
#'
#' This function replaces aou_create_temp_table() for generic OMOP CDM databases
#'
#' @param connection Database connection object
#' @param data Data frame or tibble to upload
#' @param table_name Name for the temporary table (optional)
#' @param resultsDatabaseSchema Schema for temporary tables (optional)
#'
#' @return A dplyr tbl reference to the created temporary table
#' @export
#'
#' @examples
#' \dontrun{
#' # Create temp table from local data frame
#' concept_df <- data.frame(concept_id = c(1, 2, 3))
#' temp_tbl <- create_temp_table(connection, concept_df)
#' }
create_temp_table <- function(connection,
                            data,
                            table_name = NULL,
                            resultsDatabaseSchema = NULL) {
  
  mode <- attr(connection, "mode", exact = TRUE)
  if (is.null(mode)) mode <- "generic"
  
  # Generate unique table name if not provided
  if (is.null(table_name)) {
    table_name <- paste0("temp_", format(Sys.time(), "%Y%m%d_%H%M%S_"),
                        sample(10000:99999, 1))
  }
  
  if (mode == "allofus") {
    # Use All of Us specific function if available
    if (requireNamespace("allofus", quietly = TRUE)) {
      return(allofus::aou_create_temp_table(data))
    } else {
      # Fallback to DBI for BigQuery
      DBI::dbWriteTable(connection, table_name, data, temporary = TRUE)
      return(dplyr::tbl(connection, table_name))
    }
    
  } else {
    # Generic OMOP CDM approach
    if (is.null(resultsDatabaseSchema)) {
      resultsDatabaseSchema <- attr(connection, "resultsDatabaseSchema", exact = TRUE)
    }
    
    dbms <- attr(connection, "dbms", exact = TRUE)
    
    # Handle NULL dbms - default to sql server for OHDSI compatibility
    if (is.null(dbms)) {
      dbms <- "sql server"
    }
    
    # Platform-specific temp table creation
    if (dbms %in% c("postgresql", "redshift")) {
      # PostgreSQL/Redshift: Use TEMP tables
      DBI::dbWriteTable(connection, table_name, data, temporary = TRUE)
      return(dplyr::tbl(connection, table_name))
      
    } else if (dbms == "sql server" || dbms == "pdw") {
      # SQL Server: Use # prefix for temp tables
      temp_table_name <- paste0("#", table_name)
      DatabaseConnector::insertTable(connection = connection,
                                    tableName = temp_table_name,
                                    data = data,
                                    tempTable = TRUE)
      # Use dbplyr::in_schema for proper temp table reference
      return(dplyr::tbl(connection, temp_table_name))
      
    } else if (dbms == "oracle") {
      # Oracle: Use global temporary tables or regular tables in results schema
      if (!is.null(resultsDatabaseSchema)) {
        full_table_name <- paste(resultsDatabaseSchema, table_name, sep = ".")
        DatabaseConnector::insertTable(connection = connection,
                                      tableName = full_table_name,
                                      data = data,
                                      tempTable = FALSE,
                                      dropTableIfExists = TRUE)
        return(dplyr::tbl(connection, dbplyr::sql(full_table_name)))
      }
      
    } else if (dbms == "bigquery") {
      # BigQuery (non-All of Us)
      DBI::dbWriteTable(connection, table_name, data, temporary = TRUE)
      return(dplyr::tbl(connection, table_name))
      
    } else if (dbms == "snowflake") {
      # Snowflake
      DBI::dbWriteTable(connection, table_name, data, temporary = TRUE)
      return(dplyr::tbl(connection, table_name))
      
    } else if (dbms == "spark" || dbms == "databricks") {
      # Spark/Databricks: Use managed tables with unique prefix
      # Spark doesn't support true temp tables, so we create regular tables
      # with a unique prefix to avoid conflicts
      
      # Get the schema from connection attributes - try multiple sources
      schema <- attr(connection, "resultsDatabaseSchema", exact = TRUE)
      if (is.null(schema)) {
        schema <- attr(connection, "cdmDatabaseSchema", exact = TRUE)
      }
      if (is.null(schema)) {
        # Try to get from resultsDatabaseSchema parameter
        schema <- resultsDatabaseSchema
      }
      if (is.null(schema)) {
        # Default to a temp schema if nothing else is available
        schema <- "default"
        warning("No schema specified for temp tables, using 'default'")
      }
      
      # Parse catalog and schema if in catalog.schema format
      if (grepl("\\.", schema)) {
        parts <- strsplit(schema, "\\.")[[1]]
        catalog <- parts[1]
        schema_name <- parts[2]
      } else {
        # Just schema name, use main catalog
        catalog <- "main"
        schema_name <- schema
      }
      
      # Create unique table name
      temp_table_name <- paste0("temp_", Sys.getpid(), "_", gsub("-", "_", table_name))
      
      # Create fully qualified table name for Databricks
      full_table_name <- paste(catalog, schema_name, temp_table_name, sep = ".")
      
      # Drop table if it exists using fully qualified name
      tryCatch({
        DBI::dbExecute(connection, paste0("DROP TABLE IF EXISTS ", full_table_name))
      }, error = function(e) {
        # Ignore errors if table doesn't exist
      })
      
      # Use DBI directly for better control over table creation
      # This avoids DatabaseConnector's assumptions about schema
      tryCatch({
        # Write the table using DBI which handles Spark better
        DBI::dbWriteTable(connection, 
                         dbplyr::Id(catalog = catalog, 
                                    schema = schema_name, 
                                    table = temp_table_name),
                         data,
                         overwrite = TRUE,
                         temporary = FALSE)
      }, error = function(e) {
        # Fallback to DatabaseConnector if DBI fails
        message("Note: Using DatabaseConnector fallback for table creation")
        DatabaseConnector::insertTable(connection = connection,
                                      databaseSchema = paste(catalog, schema_name, sep = "."),
                                      tableName = temp_table_name,
                                      data = data,
                                      tempTable = FALSE,
                                      dropTableIfExists = TRUE)
      })
      
      # Return table reference - try using SQL string for better qualification
      # This ensures the full catalog.schema.table name is used in SQL generation
      full_sql_ref <- paste(catalog, schema_name, temp_table_name, sep = ".")
      
      # Use sql() to create a SQL literal that will be used as-is in queries
      return(dplyr::tbl(connection, dbplyr::sql(full_sql_ref)))
      
    } else {
      # Default fallback
      DatabaseConnector::insertTable(connection = connection,
                                    tableName = table_name,
                                    data = data,
                                    tempTable = TRUE)
      return(dplyr::tbl(connection, table_name))
    }
  }
}

#' Compute and store query results
#'
#' This function replaces aou_compute() for generic OMOP CDM databases.
#' It executes a lazy query and stores results in a temporary table.
#'
#' @param lazy_query A lazy dplyr query
#' @param connection Database connection object (optional, extracted from query)
#' @param name Name for the result table (optional)
#' @param temporary Whether to create a temporary table (default TRUE)
#'
#' @return A dplyr tbl reference to the computed table
#' @export
#'
#' @examples
#' \dontrun{
#' # Compute a lazy query
#' person_tbl <- get_cdm_table(connection, "person")
#' adults <- person_tbl %>% filter(year_of_birth < 2000)
#' adults_computed <- compute_table(adults)
#' }
compute_table <- function(lazy_query,
                        connection = NULL,
                        name = NULL,
                        temporary = TRUE) {
  
  # Extract connection from lazy query if not provided
  if (is.null(connection)) {
    connection <- lazy_query$src$con
  }
  
  mode <- attr(connection, "mode", exact = TRUE)
  if (is.null(mode)) mode <- "generic"
  
  # Generate unique name if not provided
  if (is.null(name)) {
    name <- paste0("computed_", format(Sys.time(), "%Y%m%d_%H%M%S_"),
                  sample(10000:99999, 1))
  }
  
  # Clean name for database compatibility
  dbms <- attr(connection, "dbms", exact = TRUE)
  if (dbms == "spark" || dbms == "databricks") {
    # Remove any # prefix that might have been added for SQL Server
    name <- gsub("^#", "", name)
    # Ensure valid table name
    name <- gsub("[^a-zA-Z0-9_]", "_", name)
  }
  
  if (mode == "allofus") {
    # Use All of Us specific function if available
    if (requireNamespace("allofus", quietly = TRUE) && 
        exists("aou_compute", where = "package:allofus")) {
      return(allofus::aou_compute(lazy_query))
    } else {
      # Fallback to dplyr compute
      return(dplyr::compute(lazy_query, name = name, temporary = temporary))
    }
    
  } else {
    # Generic OMOP CDM approach
    dbms <- attr(connection, "dbms", exact = TRUE)
    
    # Handle NULL dbms - default to sql server for OHDSI compatibility
    if (is.null(dbms)) {
      dbms <- "sql server"
    }
    
    # Special handling for Spark/Databricks
    if (dbms == "spark" || dbms == "databricks") {
      # For Spark, we need to create the table in the correct schema
      # Use create_temp_table which handles schema properly
      
      # First, collect the data or use CREATE TABLE AS SELECT
      # Get the schema from connection attributes
      schema <- attr(connection, "resultsDatabaseSchema", exact = TRUE)
      if (is.null(schema)) {
        schema <- attr(connection, "cdmDatabaseSchema", exact = TRUE)
      }
      
      if (!is.null(schema)) {
        # Parse catalog and schema
        if (grepl("\\.", schema)) {
          parts <- strsplit(schema, "\\.")[[1]]
          catalog <- parts[1]
          schema_name <- parts[2]
        } else {
          catalog <- "main"
          schema_name <- schema
        }
        
        # Generate unique table name if not provided
        if (is.null(name) || grepl("^computed_.*_%H%M%S_", name)) {
          name <- paste0("computed_", Sys.getpid(), "_", format(Sys.time(), "%Y%m%d%H%M%S"))
        }
        
        # Clean table name for Spark - remove any # prefix and ensure valid name
        name <- gsub("^#", "", name)  # Remove SQL Server temp table prefix
        name <- gsub("[^a-zA-Z0-9_]", "_", name)  # Replace invalid characters with underscore
        
        # Create fully qualified table name
        full_table_name <- paste(catalog, schema_name, name, sep = ".")
        
        # Get the SQL query
        sql_query <- dbplyr::sql_render(lazy_query)
        
        # Create table using CREATE TABLE AS SELECT
        create_sql <- paste0("CREATE OR REPLACE TABLE ", full_table_name, " AS ", sql_query)
        
        # Execute the CREATE TABLE statement
        DBI::dbExecute(connection, create_sql)
        
        # Return reference to the new table using in_catalog
        return(dplyr::tbl(connection, 
                         dbplyr::in_catalog(catalog, schema_name, name)))
      } else {
        # Fallback to standard compute if no schema available
        warning("No schema specified for Spark/Databricks compute_table. Results may fail.")
        result <- dplyr::compute(lazy_query, name = name, temporary = FALSE)
        return(result)
      }
      
    } else if (dbms %in% c("sql server", "pdw") && temporary) {
      # SQL Server requires # prefix for temp tables
      name <- paste0("#", gsub("^#", "", name))
      # Use dplyr compute with platform-appropriate settings
      result <- dplyr::compute(lazy_query,
                             name = name,
                             temporary = temporary)
      return(result)
    } else {
      # Use dplyr compute with platform-appropriate settings
      result <- dplyr::compute(lazy_query,
                             name = name,
                             temporary = temporary)
      return(result)
    }
  }
}

#' Execute raw SQL query
#'
#' Executes a SQL query with proper rendering and translation for the platform
#'
#' @param connection Database connection object
#' @param sql SQL query string (can use SqlRender markup)
#' @param cdmDatabaseSchema CDM schema name
#' @param resultsDatabaseSchema Results schema name
#' @param ... Additional parameters for SqlRender
#'
#' @return Query results as a data frame
#' @export
execute_query <- function(connection,
                        sql,
                        cdmDatabaseSchema = NULL,
                        resultsDatabaseSchema = NULL,
                        ...) {
  
  mode <- attr(connection, "mode", exact = TRUE)
  if (is.null(mode)) mode <- "generic"
  
  # Get schemas from connection attributes if not provided
  if (is.null(cdmDatabaseSchema)) {
    cdmDatabaseSchema <- attr(connection, "cdmDatabaseSchema", exact = TRUE)
  }
  if (is.null(resultsDatabaseSchema)) {
    resultsDatabaseSchema <- attr(connection, "resultsDatabaseSchema", exact = TRUE)
  }
  
  if (mode == "allofus") {
    # Direct SQL execution for All of Us
    if (requireNamespace("allofus", quietly = TRUE) && 
        exists("aou_sql", where = "package:allofus")) {
      return(allofus::aou_sql(sql))
    } else {
      return(DBI::dbGetQuery(connection, sql))
    }
    
  } else {
    # Generic OMOP CDM with SqlRender
    dbms <- attr(connection, "dbms", exact = TRUE)
    
    # Handle NULL dbms - default to sql server for OHDSI compatibility
    if (is.null(dbms)) {
      dbms <- "sql server"
    }
    
    # Render SQL with parameters
    rendered_sql <- SqlRender::render(sql,
                                     cdm_database_schema = cdmDatabaseSchema,
                                     results_database_schema = resultsDatabaseSchema,
                                     ...)
    
    # Translate to target dialect
    translated_sql <- SqlRender::translate(rendered_sql,
                                          targetDialect = dbms)
    
    # Execute query
    result <- DatabaseConnector::querySql(connection, translated_sql)
    
    # Convert column names to lowercase for consistency
    names(result) <- tolower(names(result))
    
    return(result)
  }
}

#' Drop a temporary table
#'
#' Removes a temporary table from the database
#'
#' @param connection Database connection object
#' @param table_name Name of the table to drop
#' @param resultsDatabaseSchema Schema containing the table (optional)
#'
#' @return NULL
#' @export
drop_temp_table <- function(connection,
                          table_name,
                          resultsDatabaseSchema = NULL) {
  
  mode <- attr(connection, "mode", exact = TRUE)
  if (is.null(mode)) mode <- "generic"
  
  if (mode == "allofus") {
    # Drop table in BigQuery
    sql <- paste("DROP TABLE IF EXISTS", table_name)
    DBI::dbExecute(connection, sql)
    
  } else {
    # Generic OMOP CDM
    dbms <- attr(connection, "dbms", exact = TRUE)
    
    # Handle NULL dbms - default to sql server for OHDSI compatibility
    if (is.null(dbms)) {
      dbms <- "sql server"
    }
    
    if (dbms %in% c("sql server", "pdw")) {
      # SQL Server temp table
      if (!startsWith(table_name, "#")) {
        table_name <- paste0("#", table_name)
      }
      sql <- paste("DROP TABLE IF EXISTS", table_name)
      
    } else if (!is.null(resultsDatabaseSchema)) {
      # Schema-qualified table
      sql <- paste("DROP TABLE IF EXISTS",
                  paste(resultsDatabaseSchema, table_name, sep = "."))
    } else {
      # Simple drop
      sql <- paste("DROP TABLE IF EXISTS", table_name)
    }
    
    # Translate and execute
    sql <- SqlRender::translate(sql, targetDialect = dbms)
    DatabaseConnector::executeSql(connection, sql)
  }
  
  message(paste("Dropped table:", table_name))
  return(invisible(NULL))
}

#' Helper function to handle date differences across platforms
#'
#' Provides consistent date difference calculation across database platforms
#' using SqlRender for proper translation to target dialects.
#'
#' @param date1 First date column
#' @param date2 Second date column
#' @param units Units for difference ("day", "month", "year")
#' @param connection Optional database connection for dialect detection
#'
#' @return SQL expression for date difference
#' @export
date_diff_sql <- function(date1, date2, units = "day", connection = NULL) {
  # Use the SqlRender-based wrapper for cross-platform compatibility
  return(sql_date_diff(date1, date2, units, connection))
}

#' Wrapper for date_diff function used in original code
#'
#' Maintains compatibility with original algorithm code while providing
#' cross-platform support through SqlRender.
#'
#' @param date1 First date
#' @param date2 Second date
#' @param unit SQL unit expression
#' @param connection Optional database connection for dialect detection
#'
#' @return Date difference expression
#' @export
date_diff <- function(date1, date2, unit, connection = NULL) {
  # Parse the unit string to determine the time unit
  unit_str <- as.character(unit)
  
  if (grepl("day", unit_str, ignore.case = TRUE)) {
    units <- "day"
  } else if (grepl("month", unit_str, ignore.case = TRUE)) {
    units <- "month"
  } else if (grepl("year", unit_str, ignore.case = TRUE)) {
    units <- "year"
  } else {
    units <- "day"  # default
  }
  
  # Use the SqlRender-based wrapper for cross-platform compatibility
  return(sql_date_diff(date1, date2, units, connection))
}
