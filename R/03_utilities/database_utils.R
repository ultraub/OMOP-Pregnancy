#' Database Utility Functions for OMOP Pregnancy
#'
#' Database-agnostic utilities following OHDSI standards
#' for compute operations and temp table management.

#' Database-Agnostic Compute Wrapper
#'
#' Creates a temporary table in the database, handling platform-specific
#' requirements following OHDSI DatabaseConnector standards.
#' This is equivalent to All of Us's aou_compute() but works across all databases.
#'
#' @param x A lazy tbl to compute
#' @param connection Database connection (if not provided, extracted from x)
#' @param name Optional name for the temp table
#' @param temporary Whether to create a temporary table (default TRUE)
#' @param ... Additional arguments passed to compute()
#'
#' @return A lazy tbl referencing the computed temp table
#' @export
omop_compute <- function(x, connection = NULL, name = NULL, temporary = TRUE, ...) {
  
  # Extract connection if not provided
  if (is.null(connection)) {
    connection <- x$src$con
  }
  
  # Check if this database requires temp table emulation
  # (Oracle, Impala, and some others don't have true temp tables)
  requires_emulation <- tryCatch({
    DatabaseConnector::requiresTempEmulation(connection)
  }, error = function(e) {
    # If DatabaseConnector function not available, assume no emulation needed
    FALSE
  })
  
  if (requires_emulation) {
    # Ensure temp emulation schema is set for platforms that need it
    tryCatch({
      DatabaseConnector::assertTempEmulationSchemaSet()
    }, error = function(e) {
      # Check if option is set
      temp_schema <- getOption("sqlRenderTempEmulationSchema")
      if (is.null(temp_schema)) {
        warning("Temp table emulation required but no schema set. ",
                "Set option(sqlRenderTempEmulationSchema = 'your_schema')")
      }
    })
  }
  
  # Use standard compute() which DatabaseConnector will handle appropriately
  result <- tryCatch({
    compute(x, name = name, temporary = temporary, ...)
  }, error = function(e) {
    # Fallback to basic compute if advanced features not available
    if (requireNamespace("dbplyr", quietly = TRUE)) {
      dbplyr::compute(x, name = name, temporary = temporary, ...)
    } else {
      stop("compute() requires dbplyr package")
    }
  })
  
  # Track temp tables for cleanup if emulated
  if (requires_emulation && temporary) {
    # Store in session-specific list for later cleanup
    temp_tables <- getOption("omop_pregnancy_temp_tables", list())
    temp_tables[[length(temp_tables) + 1]] <- result
    options(omop_pregnancy_temp_tables = temp_tables)
  }
  
  return(result)
}

#' Clean Up Temporary Tables
#'
#' Drops all emulated temporary tables created during the session.
#' Only does something on platforms that require temp table emulation.
#'
#' @param connection Database connection
#'
#' @return NULL (invisibly)
#' @export
cleanup_temp_tables <- function(connection) {
  
  # Only cleanup on platforms that emulate temp tables
  requires_emulation <- tryCatch({
    DatabaseConnector::requiresTempEmulation(connection)
  }, error = function(e) {
    FALSE
  })
  
  if (requires_emulation) {
    message("Cleaning up emulated temporary tables...")
    
    # Use DatabaseConnector's cleanup function
    tryCatch({
      DatabaseConnector::dropEmulatedTempTables(connection)
      message("Temporary tables cleaned up successfully")
    }, error = function(e) {
      warning("Could not clean up temp tables: ", e$message)
    })
    
    # Clear our tracking list
    options(omop_pregnancy_temp_tables = list())
  }
  
  invisible(NULL)
}

#' Check if Temp Table Emulation is Required
#'
#' Helper function to check if the current database requires temp table emulation.
#'
#' @param connection Database connection
#'
#' @return Logical indicating if emulation is required
#' @export
requires_temp_emulation <- function(connection) {
  tryCatch({
    DatabaseConnector::requiresTempEmulation(connection)
  }, error = function(e) {
    # If DatabaseConnector not available or doesn't have this function,
    # try to detect based on database type
    db_type <- class(connection)[1]
    
    # Known databases that require emulation
    emulation_required <- db_type %in% c(
      "Oracle", "OracleConnection",
      "Impala", "ImpalaConnection",
      "Spark", "SparkConnection"
    )
    
    return(emulation_required)
  })
}

#' Set Temp Emulation Schema
#'
#' Sets the schema for temp table emulation on platforms that require it.
#'
#' @param schema_name Name of the schema with write privileges
#'
#' @return NULL (invisibly)
#' @export
set_temp_emulation_schema <- function(schema_name) {
  options(sqlRenderTempEmulationSchema = schema_name)
  message(sprintf("Temp emulation schema set to: %s", schema_name))
  invisible(NULL)
}

#' Compute if Lazy
#'
#' Helper function to compute a lazy tbl if needed.
#' This simplifies the pattern of checking and computing.
#'
#' @param x A tbl (lazy or local)
#' @param connection Optional database connection
#'
#' @return The computed tbl (or original if already local)
#' @export
compute_if_lazy <- function(x, connection = NULL) {
  if ("tbl_lazy" %in% class(x) || "tbl_sql" %in% class(x)) {
    return(omop_compute(x, connection))
  }
  return(x)
}

#' Paginated Collection
#'
#' Collects data from database with pagination to prevent memory overflow.
#' Matches the All of Us pattern of collect(page_size = n).
#'
#' @param x A lazy tbl to collect
#' @param page_size Number of rows to fetch per page (default 50000)
#' @param n Maximum number of rows to collect (Inf for all)
#'
#' @return A local tibble
#' @export
paginated_collect <- function(x, page_size = 50000, n = Inf) {
  
  # For compatibility, check if the database connection supports page_size
  tryCatch({
    # Try with page_size parameter (works with some databases)
    result <- collect(x, n = n, page_size = page_size)
  }, error = function(e) {
    # Fallback to regular collect without pagination
    if (grepl("page_size", e$message)) {
      warning("Database doesn't support page_size parameter, collecting without pagination")
      result <- collect(x, n = n)
    } else {
      stop(e)
    }
  })
  
  return(result)
}