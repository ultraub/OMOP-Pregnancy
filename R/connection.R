#' Database Connection Management for OMOP CDM
#'
#' Functions to manage database connections across different OMOP CDM platforms
#' while maintaining compatibility with All of Us-specific functions.
#'
#' @name connection
#' @import DBI
#' @import DatabaseConnector
NULL

#' Create a connection to an OMOP CDM database
#'
#' This function replaces aou_connect() for generic OMOP CDM databases
#' while maintaining similar functionality.
#'
#' @param connectionDetails DatabaseConnector connection details object or NULL for All of Us
#' @param mode Character string: "generic" (default) or "allofus"
#' @param cdmDatabaseSchema Character string specifying the CDM schema
#' @param resultsDatabaseSchema Character string specifying the results schema (optional)
#'
#' @return A database connection object
#' @export
#'
#' @examples
#' \dontrun{
#' # For generic OMOP CDM
#' connectionDetails <- createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "localhost/cdm",
#'   user = "user",
#'   password = "password"
#' )
#' con <- create_connection(connectionDetails)
#'
#' # For Databricks/Spark using JDBC
#' connectionDetails <- createConnectionDetails(
#'   dbms = "spark",
#'   connectionString = "jdbc:databricks://[workspace].cloud.databricks.com:443/default;
#'                       transportMode=http;ssl=1;AuthMech=3;
#'                       httpPath=/sql/1.0/warehouses/[warehouse_id]",
#'   user = "token",
#'   password = "[personal_access_token]"
#' )
#' con <- create_connection(connectionDetails,
#'                         cdmDatabaseSchema = "omop_cdm",
#'                         resultsDatabaseSchema = "omop_results")
#'
#' # For Databricks using ODBC
#' connectionDetails <- createConnectionDetails(
#'   dbms = "spark",
#'   server = "[workspace].cloud.databricks.com",
#'   port = 443,
#'   extraSettings = "HTTPPath=/sql/1.0/warehouses/[warehouse_id];
#'                    SSL=1;ThriftTransport=2;AuthMech=3",
#'   user = "token",
#'   password = "[personal_access_token]"
#' )
#' con <- create_connection(connectionDetails)
#'
#' # For All of Us (when running in that environment)
#' con <- create_connection(mode = "allofus")
#' }
create_connection <- function(connectionDetails = NULL,
                            mode = "generic",
                            cdmDatabaseSchema = NULL,
                            resultsDatabaseSchema = NULL) {
  
  if (mode == "allofus") {
    # Check if allofus package is available
    if (requireNamespace("allofus", quietly = TRUE)) {
      message("Using All of Us connection mode")
      con <- allofus::aou_connect()
      
      # Store connection metadata
      attr(con, "mode") <- "allofus"
      attr(con, "cdmDatabaseSchema") <- cdmDatabaseSchema
      attr(con, "resultsDatabaseSchema") <- resultsDatabaseSchema
      
      return(con)
    } else {
      stop("All of Us mode requested but 'allofus' package not available")
    }
  } else {
    # Generic OMOP CDM connection
    if (is.null(connectionDetails)) {
      stop("connectionDetails required for generic OMOP CDM connection")
    }
    
    message("Creating generic OMOP CDM connection")
    con <- DatabaseConnector::connect(connectionDetails)
    
    # Store connection metadata
    attr(con, "mode") <- "generic"
    attr(con, "cdmDatabaseSchema") <- cdmDatabaseSchema
    attr(con, "resultsDatabaseSchema") <- resultsDatabaseSchema
    attr(con, "dbms") <- connectionDetails$dbms
    
    # Set Spark-specific options to prevent SQL Server syntax
    if (connectionDetails$dbms == "spark" || connectionDetails$dbms == "databricks") {
      # Prevent # prefix for temp tables
      options(dbplyr.compute.defaults = list(temporary = FALSE))
      options(dbplyr.temp_prefix = "temp_")
      # Ensure Spark SQL dialect is used
      options(sqlRenderTempEmulationSchema = resultsDatabaseSchema)
      
      # Configure Arrow settings to prevent memory issues
      # Disable Arrow optimization to avoid memory initialization errors
      options(sparklyr.arrow = FALSE)
      
      # Set Java memory options if not already set
      if (is.null(getOption("sparklyr.java.options"))) {
        options(sparklyr.java.options = "-Xmx4g")
      }
      
      # For JDBC connections, try to disable Arrow at connection level
      tryCatch({
        if (!is.null(connectionDetails$connectionString) && 
            grepl("jdbc", connectionDetails$connectionString, ignore.case = TRUE)) {
          # Try to set Arrow disabled via JDBC property
          DBI::dbExecute(con, "SET spark.sql.execution.arrow.enabled = false")
        }
      }, error = function(e) {
        # Ignore if setting fails - not all Spark versions support this
        message("Note: Could not disable Arrow optimization via SQL. Continuing...")
      })
    }
    
    return(con)
  }
}

#' Validate OMOP CDM connection and structure
#'
#' Checks that required tables exist and have expected structure
#'
#' @param connection Database connection object
#' @param cdmDatabaseSchema CDM schema name
#' @param required_tables Character vector of required table names
#'
#' @return Logical TRUE if validation passes, error otherwise
#' @export
validate_connection <- function(connection,
                              cdmDatabaseSchema = NULL,
                              required_tables = c("person", "observation", 
                                                "measurement", "condition_occurrence",
                                                "procedure_occurrence", "visit_occurrence",
                                                "concept")) {
  
  mode <- attr(connection, "mode", exact = TRUE)
  if (is.null(mode)) mode <- "generic"
  
  if (mode == "allofus") {
    # For All of Us, use bigrquery to check tables
    tables <- DBI::dbListTables(connection)
    missing_tables <- setdiff(required_tables, tables)
    
    if (length(missing_tables) > 0) {
      stop(paste("Missing required tables:", paste(missing_tables, collapse = ", ")))
    }
    
  } else {
    # For generic OMOP CDM, use DatabaseConnector
    if (is.null(cdmDatabaseSchema)) {
      cdmDatabaseSchema <- attr(connection, "cdmDatabaseSchema", exact = TRUE)
      if (is.null(cdmDatabaseSchema)) {
        stop("cdmDatabaseSchema must be specified")
      }
    }
    
    # Get list of tables in the CDM schema
    tables <- DatabaseConnector::getTableNames(connection, cdmDatabaseSchema)
    tables <- tolower(tables)
    
    missing_tables <- setdiff(required_tables, tables)
    
    if (length(missing_tables) > 0) {
      stop(paste("Missing required tables in", cdmDatabaseSchema, ":",
                 paste(missing_tables, collapse = ", ")))
    }
  }
  
  message("Connection validation successful")
  return(TRUE)
}

#' Get CDM version from database
#'
#' Detects the CDM version to handle version-specific differences
#'
#' @param connection Database connection object
#' @param cdmDatabaseSchema CDM schema name
#'
#' @return Character string with CDM version (e.g., "5.3", "5.4")
#' @export
get_cdm_version <- function(connection, cdmDatabaseSchema = NULL) {
  
  mode <- attr(connection, "mode", exact = TRUE)
  if (is.null(mode)) mode <- "generic"
  
  if (mode == "allofus") {
    # All of Us typically uses CDM 5.3
    # Could query cdm_source table if available
    tryCatch({
      version_query <- "SELECT cdm_version FROM cdm_source LIMIT 1"
      result <- DBI::dbGetQuery(connection, version_query)
      if (nrow(result) > 0) {
        return(as.character(result$cdm_version[1]))
      }
    }, error = function(e) {
      message("Could not detect CDM version, assuming 5.3")
    })
    return("5.3")
    
  } else {
    # Generic OMOP CDM version detection
    if (is.null(cdmDatabaseSchema)) {
      cdmDatabaseSchema <- attr(connection, "cdmDatabaseSchema", exact = TRUE)
    }
    
    tryCatch({
      sql <- "SELECT cdm_version FROM @cdm_database_schema.cdm_source"
      sql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchema)
      sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
      
      result <- DatabaseConnector::querySql(connection, sql)
      if (nrow(result) > 0) {
        return(as.character(result$CDM_VERSION[1]))
      }
    }, error = function(e) {
      message("Could not detect CDM version, assuming 5.3")
    })
    
    return("5.3")
  }
}

#' Get a reference to a CDM table
#'
#' This function replaces tbl(con, "table_name") and provides
#' a consistent interface across All of Us and generic OMOP CDM
#'
#' @param connection Database connection object
#' @param table_name Name of the CDM table
#' @param cdmDatabaseSchema CDM schema name (for generic mode)
#'
#' @return A dplyr tbl reference to the table
#' @export
get_cdm_table <- function(connection, table_name, cdmDatabaseSchema = NULL) {
  
  mode <- attr(connection, "mode", exact = TRUE)
  if (is.null(mode)) mode <- "generic"
  
  if (mode == "allofus") {
    # Direct table reference for All of Us
    return(dplyr::tbl(connection, table_name))
    
  } else {
    # Schema-qualified reference for generic OMOP CDM
    if (is.null(cdmDatabaseSchema)) {
      cdmDatabaseSchema <- attr(connection, "cdmDatabaseSchema", exact = TRUE)
      if (is.null(cdmDatabaseSchema)) {
        stop("cdmDatabaseSchema must be specified for generic OMOP CDM")
      }
    }
    
    # Create schema-qualified table reference
    dbms <- attr(connection, "dbms", exact = TRUE)
    
    if (dbms %in% c("spark", "databricks")) {
      # Databricks uses three-level namespace: catalog.schema.table
      # Parse the cdmDatabaseSchema to handle both formats
      if (grepl("\\.", cdmDatabaseSchema)) {
        # Already in catalog.schema format
        table_ref <- dplyr::tbl(connection, 
                                dbplyr::in_catalog(
                                  strsplit(cdmDatabaseSchema, "\\.")[[1]][1],
                                  strsplit(cdmDatabaseSchema, "\\.")[[1]][2],
                                  table_name
                                ))
      } else {
        # Just schema name, use default catalog
        table_ref <- dplyr::tbl(connection, 
                                dbplyr::in_catalog("main", cdmDatabaseSchema, table_name))
      }
    } else if (dbms %in% c("postgresql", "redshift", "snowflake")) {
      table_ref <- dplyr::tbl(connection, 
                              dbplyr::in_schema(cdmDatabaseSchema, table_name))
    } else {
      # For SQL Server, Oracle, etc.
      full_table_name <- paste(cdmDatabaseSchema, table_name, sep = ".")
      table_ref <- dplyr::tbl(connection, full_table_name)
    }
    
    return(table_ref)
  }
}

#' Close database connection
#'
#' Properly closes the database connection
#'
#' @param connection Database connection object
#'
#' @return NULL
#' @export
close_connection <- function(connection) {
  
  mode <- attr(connection, "mode", exact = TRUE)
  if (is.null(mode)) mode <- "generic"
  
  if (mode == "allofus") {
    # For All of Us/BigQuery
    DBI::dbDisconnect(connection)
    
  } else {
    # For generic OMOP CDM
    DatabaseConnector::disconnect(connection)
  }
  
  message("Connection closed")
  return(invisible(NULL))
}
