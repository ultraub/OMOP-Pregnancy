#' OHDSI-Compliant Temporary Table Management
#'
#' This module provides functions for managing temporary tables following
#' OHDSI best practices, including proper handling of emulated temp tables
#' for platforms like Spark/Databricks that don't support true temp tables.
#'
#' @name temp_table_utils
#' @import DatabaseConnector
#' @import SqlRender
#' @import dplyr
NULL

# Track temp tables for cleanup (module-level variable)
.temp_tables <- new.env(parent = emptyenv())

#' Create OHDSI-compliant temporary table
#'
#' Creates a temporary table following OHDSI conventions, using '#' prefix
#' for table names and handling platform-specific differences.
#'
#' @param connection Database connection
#' @param data Data frame or lazy query to upload
#' @param table_name Name for temp table (without # prefix)
#' @param resultsDatabaseSchema Schema for results (required for some platforms)
#' @param overwrite Whether to overwrite existing table
#'
#' @return dplyr tbl reference to the created temporary table
#' @export
#'
#' @examples
#' \dontrun{
#' temp_tbl <- ohdsi_create_temp_table(
#'   connection, 
#'   data, 
#'   "my_temp_table"
#' )
#' }
ohdsi_create_temp_table <- function(connection, 
                                  data, 
                                  table_name = NULL,
                                  resultsDatabaseSchema = NULL,
                                  overwrite = TRUE) {
  
  # Validate inputs
  if (is.null(connection)) {
    stop("Connection is required")
  }
  
  if (is.null(data)) {
    warning("Received NULL data, returning empty data frame")
    return(data.frame())
  }
  
  # Generate table name if not provided
  if (is.null(table_name)) {
    table_name <- paste0("temp_", format(Sys.time(), "%Y%m%d_%H%M%S_"),
                        sample(10000:99999, 1))
  }
  
  # Ensure table name follows OHDSI convention (# prefix for temp tables)
  if (!grepl("^#", table_name)) {
    table_name <- paste0("#", table_name)
  }
  
  # Get database platform
  dbms <- attr(connection, "dbms", exact = TRUE)
  if (is.null(dbms)) {
    dbms <- "sql server"  # OHDSI default
  }
  
  # Get results schema if not provided
  if (is.null(resultsDatabaseSchema)) {
    resultsDatabaseSchema <- attr(connection, "resultsDatabaseSchema", exact = TRUE)
    if (is.null(resultsDatabaseSchema)) {
      resultsDatabaseSchema <- attr(connection, "cdmDatabaseSchema", exact = TRUE)
    }
  }
  
  # Handle platform-specific temp table creation
  if (dbms %in% c("spark", "databricks")) {
    # Spark/Databricks: Use temporary views or managed tables
    created_table <- create_spark_temp_table(
      connection, data, table_name, resultsDatabaseSchema, overwrite
    )
  } else if (dbms %in% c("oracle", "bigquery")) {
    # Oracle/BigQuery: Use emulated temp tables
    created_table <- create_emulated_temp_table(
      connection, data, table_name, resultsDatabaseSchema, overwrite
    )
  } else {
    # SQL Server, PostgreSQL, etc.: Use native temp tables
    created_table <- create_native_temp_table(
      connection, data, table_name, overwrite
    )
  }
  
  # Track for cleanup
  register_temp_table(connection, table_name, dbms)
  
  return(created_table)
}

#' Create native temporary table
#'
#' Creates a true temporary table for platforms that support them.
#'
#' @keywords internal
create_native_temp_table <- function(connection, data, table_name, overwrite) {
  
  # For SQL Server and similar platforms
  if (inherits(data, c("tbl_lazy", "tbl_sql"))) {
    # Create from lazy query using CREATE TABLE AS SELECT
    sql <- sprintf("SELECT * INTO %s FROM (%s) AS subquery",
                  table_name,
                  dbplyr::sql_render(data))
    
    if (overwrite) {
      # Drop if exists
      drop_sql <- sprintf("IF OBJECT_ID('tempdb..%s') IS NOT NULL DROP TABLE %s",
                         table_name, table_name)
      tryCatch(
        DatabaseConnector::executeSql(connection, drop_sql),
        error = function(e) { }  # Ignore if doesn't exist
      )
    }
    
    DatabaseConnector::executeSql(connection, sql)
  } else {
    # Upload local data frame
    # Remove # prefix for DBI operations
    dbi_table_name <- gsub("^#", "", table_name)
    DBI::dbWriteTable(connection, dbi_table_name, data, 
                     temporary = TRUE, overwrite = overwrite)
  }
  
  return(dplyr::tbl(connection, table_name))
}

#' Create Spark/Databricks temporary table
#'
#' Creates a temporary view or managed table for Spark/Databricks.
#'
#' @keywords internal
create_spark_temp_table <- function(connection, data, table_name, 
                                   resultsDatabaseSchema, overwrite) {
  
  # Remove # prefix as Spark doesn't use it
  spark_table_name <- gsub("^#", "", table_name)
  
  if (inherits(data, c("tbl_lazy", "tbl_sql"))) {
    # Create temporary view from lazy query
    view_sql <- sprintf("CREATE OR REPLACE TEMPORARY VIEW %s AS %s",
                       spark_table_name,
                       dbplyr::sql_render(data))
    
    DatabaseConnector::executeSql(connection, view_sql)
    
  } else {
    # For local data, create managed table in results schema
    if (!is.null(resultsDatabaseSchema)) {
      full_table_name <- paste(resultsDatabaseSchema, spark_table_name, sep = ".")
    } else {
      full_table_name <- spark_table_name
    }
    
    # First drop if exists and overwrite is TRUE
    if (overwrite) {
      drop_sql <- sprintf("DROP TABLE IF EXISTS %s", full_table_name)
      tryCatch(
        DatabaseConnector::executeSql(connection, drop_sql),
        error = function(e) { }  # Ignore if doesn't exist
      )
    }
    
    # Upload data
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = full_table_name,
      data = data,
      createTable = TRUE,
      tempTable = FALSE  # Can't use TRUE for Spark
    )
  }
  
  # Return reference with OHDSI # prefix for consistency
  return(dplyr::tbl(connection, dbplyr::in_schema(resultsDatabaseSchema, spark_table_name)))
}

#' Create emulated temporary table
#'
#' Creates an emulated temporary table for platforms without temp table support.
#'
#' @keywords internal
create_emulated_temp_table <- function(connection, data, table_name, 
                                      resultsDatabaseSchema, overwrite) {
  
  # Remove # prefix and add unique suffix
  emulated_table_name <- gsub("^#", "", table_name)
  emulated_table_name <- paste0(emulated_table_name, "_", 
                               format(Sys.time(), "%Y%m%d%H%M%S"))
  
  if (!is.null(resultsDatabaseSchema)) {
    full_table_name <- paste(resultsDatabaseSchema, emulated_table_name, sep = ".")
  } else {
    full_table_name <- emulated_table_name
  }
  
  # Drop if exists and overwrite is TRUE
  if (overwrite) {
    drop_sql <- sprintf("DROP TABLE IF EXISTS %s", full_table_name)
    tryCatch(
      DatabaseConnector::executeSql(connection, drop_sql),
      error = function(e) { }  # Ignore if doesn't exist
    )
  }
  
  if (inherits(data, c("tbl_lazy", "tbl_sql"))) {
    # Create from lazy query
    create_sql <- sprintf("CREATE TABLE %s AS %s",
                         full_table_name,
                         dbplyr::sql_render(data))
    DatabaseConnector::executeSql(connection, create_sql)
  } else {
    # Upload local data
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = full_table_name,
      data = data,
      createTable = TRUE,
      tempTable = FALSE
    )
  }
  
  return(dplyr::tbl(connection, dbplyr::in_schema(resultsDatabaseSchema, emulated_table_name)))
}

#' Register temporary table for tracking
#'
#' @keywords internal
register_temp_table <- function(connection, table_name, dbms) {
  conn_id <- as.character(substitute(connection))
  
  if (!exists(conn_id, envir = .temp_tables)) {
    .temp_tables[[conn_id]] <- list()
  }
  
  .temp_tables[[conn_id]][[table_name]] <- list(
    name = table_name,
    dbms = dbms,
    created = Sys.time()
  )
}

#' Drop OHDSI temporary table
#'
#' Drops a temporary table, handling platform differences.
#'
#' @param connection Database connection
#' @param table_name Name of temp table to drop
#'
#' @export
ohdsi_drop_temp_table <- function(connection, table_name) {
  
  dbms <- attr(connection, "dbms", exact = TRUE)
  if (is.null(dbms)) {
    dbms <- "sql server"
  }
  
  # Ensure # prefix
  if (!grepl("^#", table_name)) {
    table_name <- paste0("#", table_name)
  }
  
  if (dbms %in% c("spark", "databricks")) {
    # Drop view or table
    spark_name <- gsub("^#", "", table_name)
    
    # Try dropping as view first
    drop_view_sql <- sprintf("DROP VIEW IF EXISTS %s", spark_name)
    tryCatch(
      DatabaseConnector::executeSql(connection, drop_view_sql),
      error = function(e) {
        # If view drop fails, try table
        drop_table_sql <- sprintf("DROP TABLE IF EXISTS %s", spark_name)
        DatabaseConnector::executeSql(connection, drop_table_sql)
      }
    )
  } else if (dbms == "sql server") {
    # SQL Server temp table
    drop_sql <- sprintf("IF OBJECT_ID('tempdb..%s') IS NOT NULL DROP TABLE %s",
                       table_name, table_name)
    DatabaseConnector::executeSql(connection, drop_sql)
  } else {
    # Generic drop
    drop_sql <- sprintf("DROP TABLE IF EXISTS %s", table_name)
    DatabaseConnector::executeSql(connection, drop_sql)
  }
  
  # Remove from tracking
  conn_id <- as.character(substitute(connection))
  if (exists(conn_id, envir = .temp_tables)) {
    .temp_tables[[conn_id]][[table_name]] <- NULL
  }
}

#' Drop all emulated temporary tables
#'
#' Drops all tracked temporary tables for a connection. This is especially
#' important for platforms using emulated temp tables.
#'
#' @param connection Database connection
#' @param silent Whether to suppress messages
#'
#' @export
ohdsi_drop_emulated_temp_tables <- function(connection, silent = FALSE) {
  
  conn_id <- as.character(substitute(connection))
  
  if (!exists(conn_id, envir = .temp_tables)) {
    if (!silent) {
      message("No temporary tables to drop")
    }
    return(invisible(NULL))
  }
  
  tables <- .temp_tables[[conn_id]]
  
  if (length(tables) == 0) {
    if (!silent) {
      message("No temporary tables to drop")
    }
    return(invisible(NULL))
  }
  
  dropped <- 0
  failed <- 0
  
  for (table_info in tables) {
    tryCatch({
      ohdsi_drop_temp_table(connection, table_info$name)
      dropped <- dropped + 1
    }, error = function(e) {
      failed <- failed + 1
      if (!silent) {
        warning(sprintf("Failed to drop table %s: %s", 
                       table_info$name, e$message))
      }
    })
  }
  
  if (!silent) {
    message(sprintf("Dropped %d temporary tables (%d failed)", dropped, failed))
  }
  
  # Clear tracking
  .temp_tables[[conn_id]] <- list()
  
  invisible(NULL)
}

#' List temporary tables for a connection
#'
#' Returns a data frame of tracked temporary tables.
#'
#' @param connection Database connection
#'
#' @return Data frame with temp table information
#' @export
ohdsi_list_temp_tables <- function(connection) {
  
  conn_id <- as.character(substitute(connection))
  
  if (!exists(conn_id, envir = .temp_tables) || 
      length(.temp_tables[[conn_id]]) == 0) {
    return(data.frame(
      name = character(),
      dbms = character(),
      created = character(),
      stringsAsFactors = FALSE
    ))
  }
  
  tables <- .temp_tables[[conn_id]]
  
  do.call(rbind, lapply(tables, function(t) {
    data.frame(
      name = t$name,
      dbms = t$dbms,
      created = as.character(t$created),
      stringsAsFactors = FALSE
    )
  }))
}

#' Check if table name is a temporary table
#'
#' Checks if a table name follows OHDSI temp table convention.
#'
#' @param table_name Table name to check
#'
#' @return Logical indicating if table is temporary
#' @export
is_temp_table <- function(table_name) {
  grepl("^#", table_name)
}

#' Get full table name with schema
#'
#' Constructs full table name with schema prefix if needed.
#'
#' @param table_name Table name (may include # prefix)
#' @param schema Database schema
#' @param dbms Database platform
#'
#' @return Full table name with schema
#' @export
get_full_table_name <- function(table_name, schema = NULL, dbms = NULL) {
  
  # Temp tables don't use schema prefix in SQL Server
  if (is_temp_table(table_name) && dbms == "sql server") {
    return(table_name)
  }
  
  # For Spark/Databricks, remove # prefix
  if (dbms %in% c("spark", "databricks")) {
    table_name <- gsub("^#", "", table_name)
  }
  
  # Add schema if provided
  if (!is.null(schema) && !is_temp_table(table_name)) {
    return(paste(schema, table_name, sep = "."))
  }
  
  return(table_name)
}