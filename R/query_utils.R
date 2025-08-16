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
  
  # Check for empty data frame
  if (is.null(data) || nrow(data) == 0) {
    warning("create_temp_table: Received empty data frame (", 
            ifelse(is.null(data), "NULL", paste0(nrow(data), " rows")), 
            "). Returning empty local data frame.")
    # Return empty data frame instead of trying to create empty table
    if (!is.null(data)) {
      return(data)  # Return the empty data frame with its structure
    } else {
      return(data.frame())  # Return generic empty data frame
    }
  }
  
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
      
      # Pre-process data for Spark: handle NA values properly
      # Spark may interpret NA differently than R
      if ("gest_value" %in% names(data)) {
        # Convert any 0 values to NA for gest_value (0 is not a valid gestational week)
        data$gest_value[data$gest_value == 0] <- NA
        # Ensure numeric type
        data$gest_value <- as.numeric(data$gest_value)
      }
      if ("value_as_number" %in% names(data)) {
        # Ensure numeric type for value_as_number
        data$value_as_number <- as.numeric(data$value_as_number)
      }
      
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
        # This is expected behavior for Spark/Databricks and is not an error
        # DatabaseConnector has better Spark support than DBI for table creation
        if (interactive()) {
          message("Note: Using DatabaseConnector fallback for table creation (expected for Spark/Databricks)")
        }
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
      table_ref <- dplyr::tbl(connection, dbplyr::sql(full_sql_ref))
      
      # Store the actual table name as an attribute for later retrieval
      attr(table_ref, "table_name") <- full_sql_ref
      attr(table_ref, "spark_table") <- TRUE
      
      return(table_ref)
      
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
  if (!is.null(dbms) && (dbms == "spark" || dbms == "databricks")) {
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

#' Extract actual table name from a lazy table reference
#'
#' For Spark/Databricks, we need the actual table name without dbplyr's
#' complex SQL wrapping to avoid JDBC BackgroundFetcher errors.
#'
#' @param table_ref A tbl_sql or lazy table reference
#'
#' @return The actual table name as a string, or NULL if not found
get_spark_table_name <- function(table_ref) {
  
  # If it's already a string, return it
  if (is.character(table_ref)) {
    return(table_ref)
  }
  
  # Try to extract from different possible locations
  if (inherits(table_ref, "tbl_sql") || inherits(table_ref, "tbl_lazy")) {
    
    # Method 1: Check if table name is stored as attribute
    table_name <- attr(table_ref, "table_name", exact = TRUE)
    if (!is.null(table_name)) {
      return(table_name)
    }
    
    # Method 2: Try to get from src
    if (!is.null(table_ref$src)) {
      if (!is.null(table_ref$src$table)) {
        return(table_ref$src$table)
      }
      # Check for stored table name in src
      if (!is.null(table_ref$src$table_name)) {
        return(table_ref$src$table_name)
      }
    }
    
    # Method 3: Extract from lazy_query
    if (!is.null(table_ref$lazy_query) && !is.null(table_ref$lazy_query$x)) {
      # This might be a complex SQL object
      table_sql <- as.character(table_ref$lazy_query$x)
      
      # If it's a simple table name (no SELECT), return it
      if (!grepl("SELECT", table_sql, ignore.case = TRUE)) {
        return(table_sql)
      }
      
      # Try to extract table name from FROM clause
      from_match <- regexpr("FROM\\s+([^\\s\\(]+)", table_sql, ignore.case = TRUE, perl = TRUE)
      if (from_match > 0) {
        from_text <- regmatches(table_sql, from_match)
        table_name <- gsub("^FROM\\s+", "", from_text, ignore.case = TRUE)
        return(table_name)
      }
    }
    
    # Method 4: Try sql_render and parse
    tryCatch({
      sql_text <- as.character(dbplyr::sql_render(table_ref))
      # Look for the actual table name in the SQL
      # Pattern: FROM catalog.schema.table or FROM table
      matches <- regmatches(sql_text, 
                          gregexpr("FROM\\s+`?([^`\\s\\(]+\\.?[^`\\s\\(]+\\.?[^`\\s\\(]+)`?", 
                                  sql_text, ignore.case = TRUE, perl = TRUE))
      if (length(matches[[1]]) > 0) {
        table_name <- gsub("^FROM\\s+`?|`?$", "", matches[[1]][1], ignore.case = TRUE)
        return(table_name)
      }
    }, error = function(e) {
      # Continue to next method
    })
  }
  
  return(NULL)
}

#' Execute query using DatabaseConnector for better Spark support
#'
#' Helper function that uses DatabaseConnector::querySql instead of DBI::dbGetQuery
#' for better compatibility with Spark/Databricks connections.
#'
#' @param connection Database connection
#' @param sql SQL query string
#'
#' @return Query results as data frame
dc_query <- function(connection, sql) {
  # Ensure SQL is a character string
  sql_string <- as.character(sql)
  
  # Check if DatabaseConnector is available
  if (!requireNamespace("DatabaseConnector", quietly = TRUE)) {
    warning("DatabaseConnector not available, using DBI")
    return(DBI::dbGetQuery(connection, sql_string))
  }
  
  # Get the DBMS type
  dbms <- attr(connection, "dbms", exact = TRUE)
  
  # For Spark/Databricks, ensure we use DatabaseConnector
  if (!is.null(dbms) && (dbms == "spark" || dbms == "databricks")) {
    tryCatch({
      # For Spark, we need to ensure proper SQL formatting
      # Remove any dbplyr SQL class wrapping
      if (inherits(sql_string, "sql")) {
        sql_string <- as.character(sql_string)
      }
      
      result <- DatabaseConnector::querySql(connection, sql_string)
      
      # Check if result is valid
      if (is.null(result) || !is.data.frame(result)) {
        warning("DatabaseConnector returned invalid result")
        return(NULL)
      }
      
      # DatabaseConnector returns uppercase column names, convert to lowercase
      names(result) <- tolower(names(result))
      return(result)
    }, error = function(e) {
      warning("DatabaseConnector query failed for Spark: ", e$message)
      # Don't fallback to DBI for Spark - it won't work
      return(NULL)
    })
  } else {
    # For non-Spark databases, try DatabaseConnector first, then DBI
    tryCatch({
      result <- DatabaseConnector::querySql(connection, sql_string)
      names(result) <- tolower(names(result))
      return(result)
    }, error = function(e) {
      # Fallback to DBI for non-Spark connections
      tryCatch({
        return(DBI::dbGetQuery(connection, sql_string))
      }, error = function(e2) {
        warning("Query failed with both DatabaseConnector and DBI: ", e2$message)
        return(NULL)
      })
    })
  }
}

#' Spark-specific count function that avoids JDBC BackgroundFetcher errors
#'
#' Uses simple direct queries on table names instead of complex dbplyr SQL
#'
#' @param connection Database connection
#' @param table_ref Table reference or table name
#'
#' @return Row count or NA if all methods fail
spark_safe_count <- function(connection, table_ref) {
  
  # Get the actual table name
  table_name <- get_spark_table_name(table_ref)
  
  if (is.null(table_name)) {
    warning("Could not extract table name from reference")
    return(NA_real_)
  }
  
  # Method 1: Direct COUNT on table name (no subqueries)
  tryCatch({
    count_sql <- paste0("SELECT COUNT(*) AS cnt FROM ", table_name)
    
    # Use DatabaseConnector::executeSql for better Spark compatibility
    result <- DatabaseConnector::querySql(connection, count_sql)
    
    if (!is.null(result) && nrow(result) > 0) {
      # Handle different column name cases
      if ("CNT" %in% names(result)) {
        return(as.numeric(result$CNT))
      } else if ("cnt" %in% names(result)) {
        return(as.numeric(result$cnt))
      } else if (ncol(result) > 0) {
        return(as.numeric(result[[1]]))
      }
    }
  }, error = function(e) {
    # Continue to next method
  })
  
  # Method 2: Try with backticks for table name (Spark sometimes needs this)
  tryCatch({
    # Split table name into parts and wrap each in backticks
    parts <- strsplit(table_name, "\\.")[[1]]
    quoted_name <- paste0("`", parts, "`", collapse = ".")
    count_sql <- paste0("SELECT COUNT(*) FROM ", quoted_name)
    
    result <- DatabaseConnector::querySql(connection, count_sql)
    
    if (!is.null(result) && nrow(result) > 0) {
      return(as.numeric(result[[1]]))
    }
  }, error = function(e) {
    # Continue to next method
  })
  
  # Method 3: Check if table exists (at least verify it's there)
  tryCatch({
    # Just try to select 1 row to verify table accessibility
    test_sql <- paste0("SELECT 1 FROM ", table_name, " LIMIT 1")
    result <- DatabaseConnector::querySql(connection, test_sql)
    
    if (!is.null(result)) {
      # Table exists and is accessible, but we can't get exact count
      # Return -1 as a flag that table exists but count failed
      return(-1)
    }
  }, error = function(e) {
    # Table might not exist or be accessible
  })
  
  # All methods failed
  return(NA_real_)
}

#' Safe count operation for Spark/Databricks
#'
#' Gets row count from a lazy query without collecting all data.
#' Optimized for Spark/Databricks to avoid Arrow issues.
#'
#' @param lazy_query A lazy dplyr query
#'
#' @return Numeric count value
#' @export
safe_count <- function(lazy_query) {
  
  # Extract connection from lazy query
  if (inherits(lazy_query, c("tbl_lazy", "tbl_sql"))) {
    connection <- lazy_query$src$con
  } else {
    # If it's already collected, just return count
    return(nrow(lazy_query))
  }
  
  # Get the DBMS type
  dbms <- attr(connection, "dbms", exact = TRUE)
  
  # For Spark/Databricks, use the specialized Spark count function
  if (!is.null(dbms) && (dbms == "spark" || dbms == "databricks")) {
    # Use the Spark-specific count that extracts table names properly
    result <- spark_safe_count(connection, lazy_query)
    
    # If we got a valid count (including -1 for "exists but can't count"), return it
    if (!is.na(result)) {
      if (result == -1) {
        # Table exists but exact count failed - return a warning and NA
        warning("Table exists but could not get exact count from Spark")
        return(NA_real_)
      }
      return(result)
    }
    
    # Strategy 1: Try direct SQL COUNT using DatabaseConnector
    tryCatch({
      count_query <- lazy_query %>% 
        summarise(n = n()) %>%
        dbplyr::sql_render()
      
      # Debug: Log the SQL being executed
      if (interactive()) {
        cat("[DEBUG] safe_count Strategy 1 SQL: ", substring(as.character(count_query), 1, 100), "...\n")
      }
      
      result <- dc_query(connection, as.character(count_query))
      if (!is.null(result) && nrow(result) > 0) {
        return(as.numeric(result[[1]]))
      } else {
        if (interactive()) {
          cat("[DEBUG] safe_count Strategy 1 returned NULL or empty\n")
        }
      }
    }, error = function(e1) {
      if (interactive()) {
        cat("[DEBUG] safe_count Strategy 1 error: ", e1$message, "\n")
      }
      # Continue to next strategy
    })
    
    # Strategy 2: Try COUNT(*) on the rendered table with DatabaseConnector
    tryCatch({
      # Get the table SQL
      table_sql <- dbplyr::sql_render(lazy_query)
      # Wrap in COUNT(*)
      count_sql <- paste0("SELECT COUNT(*) as n FROM (", as.character(table_sql), ") AS count_query")
      result <- dc_query(connection, count_sql)
      if (!is.null(result) && nrow(result) > 0) {
        return(as.numeric(result[[1]]))
      }
    }, error = function(e2) {
      # Continue to next strategy
    })
    
    # Strategy 3: Try simpler COUNT(*) without subquery
    tryCatch({
      # If lazy_query is a simple table reference, try direct COUNT
      if (inherits(lazy_query, "tbl_sql")) {
        table_name <- as.character(lazy_query$lazy_query$x)
        count_sql <- paste0("SELECT COUNT(*) FROM ", table_name)
        result <- dc_query(connection, count_sql)
        if (!is.null(result) && nrow(result) > 0) {
          return(as.numeric(result[[1]]))
        }
      }
    }, error = function(e3) {
      # Continue to fallback
    })
    
    # Last resort: return NA with warning
    warning("Could not get count from Databricks after trying all strategies")
    return(NA_real_)
  } else {
    # For non-Spark, use standard approach with DatabaseConnector fallback
    tryCatch({
      count_query <- lazy_query %>% 
        summarise(n = n()) %>%
        dbplyr::sql_render()
      
      result <- dc_query(connection, as.character(count_query))
      if (!is.null(result) && nrow(result) > 0) {
        return(as.numeric(result[[1]]))
      } else {
        warning("Could not get count: query returned no results")
        return(NA_real_)
      }
    }, error = function(e) {
      warning("Could not get count: ", e$message)
      return(NA_real_)
    })
  }
}

#' Safe collect operation for Spark/Databricks
#'
#' Handles collect() operations with Spark-specific optimizations to avoid
#' Apache Arrow memory issues. This function provides a safe way to collect
#' data from Spark/Databricks that avoids Arrow serialization problems.
#'
#' @param lazy_query A lazy dplyr query
#' @param n_max Maximum number of rows to collect (default NULL for all)
#' @param use_temp_table For Spark, whether to use temp table approach (default TRUE)
#'
#' @return A local data frame
#' @export
safe_collect <- function(lazy_query, n_max = NULL, use_temp_table = TRUE) {
  
  # Extract connection from lazy query
  if (inherits(lazy_query, c("tbl_lazy", "tbl_sql"))) {
    connection <- lazy_query$src$con
  } else {
    # If it's already collected, just return it
    return(lazy_query)
  }
  
  # Get the DBMS type
  dbms <- attr(connection, "dbms", exact = TRUE)
  
  # For Spark/Databricks, use special collection strategy
  if (!is.null(dbms) && (dbms == "spark" || dbms == "databricks")) {
    
    # Strategy 1: Try to disable Arrow for this specific collection
    tryCatch({
      # Temporarily disable Arrow if possible (only for sparklyr connections)
      old_arrow <- NULL
      if ("sparklyr" %in% loadedNamespaces()) {
        old_arrow <- getOption("sparklyr.arrow")
        options(sparklyr.arrow = FALSE)
        on.exit({if (!is.null(old_arrow)) options(sparklyr.arrow = old_arrow)}, add = TRUE)
      }
      
      # Try direct collection with limited rows first
      if (!is.null(n_max)) {
        result <- lazy_query %>% 
          head(n_max) %>%
          collect()
      } else if (use_temp_table) {
        # Strategy 2: Use temp table approach for large datasets
        # This can help with memory issues by staging the data
        result <- tryCatch({
          temp_result <- compute_table(lazy_query, connection = connection)
          # Use DatabaseConnector for better Spark support
          sql_query <- dbplyr::sql_render(temp_result)
          result <- dc_query(connection, as.character(sql_query))
          if (!is.null(result)) {
            return(result)
          } else {
            stop("Query returned NULL")
          }
        }, error = function(e) {
          # If temp table approach fails, try direct SQL query with DatabaseConnector
          message("Temp table collection failed, trying direct SQL query...")
          sql_query <- dbplyr::sql_render(lazy_query)
          result <- dc_query(connection, as.character(sql_query))
          if (!is.null(result)) {
            return(result)
          } else {
            stop("Direct query also returned NULL")
          }
        })
      } else {
        # Strategy 3: Try direct SQL query first for Spark
        sql_query <- dbplyr::sql_render(lazy_query)
        result <- dc_query(connection, as.character(sql_query))
        if (is.null(result)) {
          stop("Query returned NULL")
        }
      }
      
      return(result)
      
    }, error = function(e) {
      # Check for various collection failure patterns - including use_cli_format
      if (grepl("Arrow|arrow|MemoryUtil|ArrowNotAvailable|Failed to collect|use_cli_format|no field", e$message, ignore.case = TRUE)) {
        message("Collection failed (", substring(e$message, 1, 50), "...), trying direct SQL fallback...")
        
        # Strategy 4: Use SQL query directly with DatabaseConnector
        tryCatch({
          sql_query <- dbplyr::sql_render(lazy_query)
          
          # For very simple queries (like count), try to execute directly
          if (grepl("COUNT\\(\\*\\)|COUNT\\(1\\)", as.character(sql_query), ignore.case = TRUE)) {
            # This is likely a count query - handle specially
            result <- tryCatch({
              dc_query(connection, as.character(sql_query))
            }, error = function(e) {
              # If even simple count fails, return empty data frame with n column
              warning("Could not execute count query: ", e$message)
              data.frame(n = 0)
            })
            if (!is.null(result)) {
              return(result)
            }
          } else {
            # For complex queries, try direct execution with DatabaseConnector
            result <- dc_query(connection, as.character(sql_query))
            if (!is.null(result)) {
              return(result)
            }
          }
        }, error = function(e2) {
          # Check if this is a count query
          sql_query <- tryCatch(dbplyr::sql_render(lazy_query), error = function(e) "")
          if (grepl("count", as.character(sql_query), ignore.case = TRUE)) {
            # If it's a count query that's failing, return 0 with warning
            warning("Could not collect count from Databricks: ", e2$message)
            return(data.frame(n = 0))
          }
          # For non-count queries, return empty data frame
          warning("Failed to collect data from Databricks: ", e2$message)
          return(data.frame())
        })
      } else {
        # Re-throw non-collection errors with context
        stop(paste("Collection failed:", e$message))
      }
    })
    
  } else {
    # For non-Spark databases, use standard collect
    if (!is.null(n_max)) {
      return(lazy_query %>% head(n_max) %>% collect())
    } else {
      return(collect(lazy_query))
    }
  }
}

#' Verify data integrity after upload to database
#'
#' Checks that critical columns have proper NULL/non-NULL values,
#' especially for HIP_concepts and similar reference tables.
#'
#' @param table_ref Database table reference (from create_temp_table)
#' @param table_name Name of the table for logging
#'
#' @return List with verification results
#' @export
verify_table_upload <- function(table_ref, table_name = "table") {
  
  cat(paste0("\n[VERIFY] Checking integrity of ", table_name, "...\n"))
  
  # Extract connection for Spark check
  connection <- NULL
  if (inherits(table_ref, c("tbl_lazy", "tbl_sql"))) {
    connection <- table_ref$src$con
  }
  
  # Get the DBMS type
  dbms <- NULL
  if (!is.null(connection)) {
    dbms <- attr(connection, "dbms", exact = TRUE)
  }
  
  # For Spark/Databricks, use special counting approach
  if (!is.null(dbms) && (dbms == "spark" || dbms == "databricks")) {
    # Use the Spark-specific count function directly
    total_count <- spark_safe_count(connection, table_ref)
    cat(paste0("  Total rows: ", 
               ifelse(is.na(total_count), "unable to count", as.character(total_count)), 
               "\n"))
  } else {
    # Get total row count using safe_count for non-Spark
    total_count <- safe_count(table_ref)
    cat(paste0("  Total rows: ", 
               ifelse(is.na(total_count), "unable to count", as.character(total_count)), 
               "\n"))
  }
  
  # Check columns
  col_names <- colnames(table_ref)
  cat(paste0("  Columns: ", paste(col_names, collapse = ", "), "\n"))
  
  # For HIP_concepts specifically, check gest_value
  if ("gest_value" %in% col_names) {
    # For Spark, try a simpler approach
    if (!is.null(dbms) && (dbms == "spark" || dbms == "databricks")) {
      # Try to get the actual table name and query directly
      actual_table <- get_spark_table_name(table_ref)
      if (!is.null(actual_table)) {
        tryCatch({
          # Query for non-NULL count
          non_null_sql <- paste0("SELECT COUNT(*) AS cnt FROM ", actual_table, " WHERE gest_value IS NOT NULL")
          result <- DatabaseConnector::querySql(connection, non_null_sql)
          non_null_count <- ifelse(!is.null(result) && nrow(result) > 0, as.numeric(result[[1]]), NA)
          
          # Query for NULL count
          null_sql <- paste0("SELECT COUNT(*) AS cnt FROM ", actual_table, " WHERE gest_value IS NULL")
          result <- DatabaseConnector::querySql(connection, null_sql)
          null_count <- ifelse(!is.null(result) && nrow(result) > 0, as.numeric(result[[1]]), NA)
          
          cat(paste0("  gest_value: ", 
                     ifelse(is.na(non_null_count), "?", as.character(non_null_count)), 
                     " non-NULL, ",
                     ifelse(is.na(null_count), "?", as.character(null_count)),
                     " NULL\n"))
        }, error = function(e) {
          cat("  gest_value: unable to count\n")
        })
      } else {
        cat("  gest_value: unable to count (table name not found)\n")
      }
    } else {
      # Standard approach for non-Spark
      # Count non-NULL gest_value
      non_null_count <- table_ref %>%
        filter(!is.na(gest_value)) %>%
        safe_count()
      
      # Count NULL gest_value
      null_count <- table_ref %>%
        filter(is.na(gest_value)) %>%
        safe_count()
      
      cat(paste0("  gest_value: ", 
                 ifelse(is.na(non_null_count), "?", as.character(non_null_count)), 
                 " non-NULL, ",
                 ifelse(is.na(null_count), "?", as.character(null_count)),
                 " NULL\n"))
    }
    
    # Sample some values
    if (!is.na(non_null_count) && non_null_count > 0) {
      sample_data <- tryCatch({
        table_ref %>%
          filter(!is.na(gest_value)) %>%
          head(5) %>%
          safe_collect()
      }, error = function(e) {
        NULL
      })
      
      if (!is.null(sample_data) && nrow(sample_data) > 0) {
        cat("  Sample gest_value values: ", 
            paste(sample_data$gest_value, collapse = ", "), "\n")
      }
    }
  }
  
  # Check value_as_number if present
  if ("value_as_number" %in% col_names) {
    # For Spark, use direct SQL query
    if (!is.null(dbms) && (dbms == "spark" || dbms == "databricks")) {
      actual_table <- get_spark_table_name(table_ref)
      if (!is.null(actual_table)) {
        tryCatch({
          non_null_sql <- paste0("SELECT COUNT(*) AS cnt FROM ", actual_table, " WHERE value_as_number IS NOT NULL")
          result <- DatabaseConnector::querySql(connection, non_null_sql)
          non_null_value <- ifelse(!is.null(result) && nrow(result) > 0, as.numeric(result[[1]]), NA)
          
          cat(paste0("  value_as_number: ", 
                     ifelse(is.na(non_null_value), "?", as.character(non_null_value)), 
                     " non-NULL\n"))
        }, error = function(e) {
          cat("  value_as_number: unable to count\n")
        })
      } else {
        cat("  value_as_number: unable to count (table name not found)\n")
      }
    } else {
      # Standard approach for non-Spark
      non_null_value <- table_ref %>%
        filter(!is.na(value_as_number)) %>%
        safe_count()
      
      cat(paste0("  value_as_number: ", 
                 ifelse(is.na(non_null_value), "?", as.character(non_null_value)), 
                 " non-NULL\n"))
    }
  }
  
  return(list(
    total_rows = total_count,
    columns = col_names,
    table_name = table_name
  ))
}
