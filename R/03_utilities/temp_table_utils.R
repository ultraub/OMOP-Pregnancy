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
# Track full schema-qualified names for Spark/Databricks
.temp_table_full_names <- new.env(parent = emptyenv())

#' Get Full Qualified Table Name
#'
#' Helper function to get consistent table names across platforms.
#' Ensures tables are referenced with the same name they were created with.
#'
#' @param connection Database connection
#' @param table_name Base table name (may include # prefix)
#' @param schema Optional schema name (used for Spark/Databricks)
#' @param dbms Optional database type (auto-detected if not provided)
#'
#' @return Properly qualified table name for the platform
#' @keywords internal
get_full_table_name <- function(connection, table_name, schema = NULL, dbms = NULL) {
  
  # Get database type if not provided
  if (is.null(dbms)) {
    dbms <- attr(connection, "dbms")
    if (is.null(dbms)) {
      dbms <- "sql server"  # Default
    }
  }
  
  # Get schema from connection if not provided
  if (is.null(schema)) {
    schema <- attr(connection, "results_schema")
    if (is.null(schema)) {
      schema <- attr(connection, "resultsDatabaseSchema")
    }
  }
  
  # Handle platform-specific naming
  if (dbms %in% c("spark", "databricks")) {
    # Remove # prefix for Spark/Databricks
    clean_name <- gsub("^#", "", table_name)
    
    # Check if already schema-qualified
    if (grepl("\\.", clean_name)) {
      return(clean_name)  # Already qualified
    }
    
    # Add schema if available
    if (!is.null(schema) && nchar(schema) > 0) {
      return(paste(schema, clean_name, sep = "."))
    } else {
      return(clean_name)
    }
    
  } else if (dbms == "sql server") {
    # Ensure # prefix for SQL Server temp tables
    if (!grepl("^#", table_name)) {
      return(paste0("#", table_name))
    } else {
      return(table_name)
    }
    
  } else {
    # Other platforms - return as-is
    return(table_name)
  }
}

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
  
  # For Spark/Databricks, also track the full qualified name if available
  if (dbms %in% c("spark", "databricks") && !is.null(resultsDatabaseSchema)) {
    # Create a unique connection ID using server and database attributes
    server <- attr(connection, "server")
    database <- attr(connection, "database")
    conn_id <- paste0("spark_", 
                     ifelse(!is.null(server), gsub("[^a-zA-Z0-9]", "_", server), "conn"),
                     "_", 
                     ifelse(!is.null(database), database, "db"))
    
    if (!exists(conn_id, envir = .temp_table_full_names)) {
      .temp_table_full_names[[conn_id]] <- list()
    }
    spark_table_name <- gsub("^#", "", table_name)
    full_name <- paste(resultsDatabaseSchema, spark_table_name, sep = ".")
    .temp_table_full_names[[conn_id]][[table_name]] <- full_name
  }
  
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
    # Use helper to get consistent naming
    full_table_name <- get_full_table_name(
      connection, 
      spark_table_name, 
      schema = resultsDatabaseSchema,
      dbms = "spark"
    )
    
    # First drop if exists and overwrite is TRUE
    if (overwrite) {
      drop_sql <- sprintf("DROP TABLE IF EXISTS %s", full_table_name)
      tryCatch(
        DatabaseConnector::executeSql(connection, drop_sql),
        error = function(e) { }  # Ignore if doesn't exist
      )
    }
    
    # Upload data with bulk loading for performance
    # Check for bulk load settings
    bulk_load <- as.logical(Sys.getenv("DATABASE_CONNECTOR_BULK_UPLOAD", "FALSE"))
    batch_size <- as.integer(Sys.getenv("DATABASE_CONNECTOR_BATCH_SIZE", "10000"))
    
    # For large datasets, process in batches
    if (nrow(data) > batch_size && !bulk_load) {
      message(sprintf("Large dataset (%d rows). Consider setting DATABASE_CONNECTOR_BULK_UPLOAD=TRUE for better performance.", nrow(data)))
    }
    
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = full_table_name,
      data = data,
      createTable = TRUE,
      tempTable = FALSE,  # Can't use TRUE for Spark
      bulkLoad = bulk_load  # Enable bulk loading if configured
    )
    
    # Store the full qualified name for cleanup
    # Create a unique connection ID using server and database attributes
    server <- attr(connection, "server")
    database <- attr(connection, "database")
    conn_id <- paste0("spark_", 
                     ifelse(!is.null(server), gsub("[^a-zA-Z0-9]", "_", server), "conn"),
                     "_", 
                     ifelse(!is.null(database), database, "db"))
    
    if (!exists(conn_id, envir = .temp_table_full_names)) {
      .temp_table_full_names[[conn_id]] <- list()
    }
    .temp_table_full_names[[conn_id]][[table_name]] <- full_table_name
  }
  
  # Return reference with OHDSI # prefix for consistency
  # If we created a view, just reference the view name
  # If we created a table, reference with schema
  if (inherits(data, c("tbl_lazy", "tbl_sql"))) {
    # Created a temporary view - no schema needed
    return(dplyr::tbl(connection, spark_table_name))
  } else if (!is.null(resultsDatabaseSchema)) {
    # Created a table with schema
    return(dplyr::tbl(connection, dbplyr::in_schema(resultsDatabaseSchema, spark_table_name)))
  } else {
    # Created a table without schema
    return(dplyr::tbl(connection, spark_table_name))
  }
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
  
  # Use helper to get consistent naming
  full_table_name <- get_full_table_name(
    connection,
    emulated_table_name,
    schema = resultsDatabaseSchema,
    dbms = attr(connection, "dbms")
  )
  
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
    # Upload local data with bulk loading for performance
    bulk_load <- as.logical(Sys.getenv("DATABASE_CONNECTOR_BULK_UPLOAD", "FALSE"))
    
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = full_table_name,
      data = data,
      createTable = TRUE,
      tempTable = FALSE,
      bulkLoad = bulk_load
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
    # Use helper function to get consistent table name
    results_schema <- attr(connection, "results_schema")
    spark_name <- get_full_table_name(connection, table_name, schema = results_schema, dbms = dbms)
    
    # Try dropping as view first (using IF EXISTS to prevent errors)
    drop_view_sql <- sprintf("DROP VIEW IF EXISTS %s", spark_name)
    view_dropped <- FALSE
    tryCatch({
      DatabaseConnector::executeSql(connection, drop_view_sql)
      view_dropped <- TRUE
    }, error = function(e) {
      # Expected if view doesn't exist - ignore
      if (!grepl("(does not exist|not found)", e$message, ignore.case = TRUE)) {
        # Log unexpected errors in debug mode only
        if (getOption("ohdsi.debug", FALSE)) {
          message(sprintf("Debug: View drop failed for %s: %s", spark_name, e$message))
        }
      }
    })
    
    # Also try dropping as table (using IF EXISTS to prevent errors)
    if (!view_dropped) {
      drop_table_sql <- sprintf("DROP TABLE IF EXISTS %s", spark_name)
      tryCatch({
        DatabaseConnector::executeSql(connection, drop_table_sql)
      }, error = function(e) {
        # Expected if table doesn't exist - ignore
        if (!grepl("(does not exist|not found|TABLE_OR_VIEW_NOT_FOUND)", e$message, ignore.case = TRUE)) {
          # Only throw error if it's not a "not found" error
          stop(e)
        }
      })
    }
    
    # Clean up stored name
    server <- attr(connection, "server")
    database <- attr(connection, "database")
    conn_id <- paste0("spark_", 
                     ifelse(!is.null(server), gsub("[^a-zA-Z0-9]", "_", server), "conn"),
                     "_", 
                     ifelse(!is.null(database), database, "db"))
    
    if (exists(conn_id, envir = .temp_table_full_names)) {
      .temp_table_full_names[[conn_id]][[table_name]] <- NULL
    }
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