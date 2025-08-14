#' Connection compatibility fixes for DatabaseConnector
#'
#' This file provides missing methods for DatabaseConnectorJdbcConnection
#' to ensure compatibility with dbplyr operations

#' SQL join suffix method for DatabaseConnector
#' 
#' @param con DatabaseConnectorJdbcConnection object
#' @param suffix Character vector of length 2 for suffixes
#' @export
sql_join_suffix.DatabaseConnectorJdbcConnection <- function(con, suffix = c(".x", ".y")) {
  suffix
}

#' Register the method
#' @import methods
#' @import DatabaseConnector
#' @import dbplyr
.onLoad <- function(libname, pkgname) {
  # Register S3 method for sql_join_suffix
  if (requireNamespace("dbplyr", quietly = TRUE) && 
      requireNamespace("DatabaseConnector", quietly = TRUE)) {
    registerS3method("sql_join_suffix", "DatabaseConnectorJdbcConnection", 
                     sql_join_suffix.DatabaseConnectorJdbcConnection,
                     envir = asNamespace("dbplyr"))
  }
}

#' Alternative approach - define the method directly
if (!exists("sql_join_suffix.DatabaseConnectorJdbcConnection")) {
  sql_join_suffix.DatabaseConnectorJdbcConnection <- function(con, suffix = c(".x", ".y")) {
    suffix
  }
  
  # Try to register it
  tryCatch({
    registerS3method("sql_join_suffix", "DatabaseConnectorJdbcConnection", 
                     sql_join_suffix.DatabaseConnectorJdbcConnection,
                     envir = asNamespace("dbplyr"))
  }, error = function(e) {
    # If registration fails, at least the function exists
    message("Note: Could not register sql_join_suffix method, but function is available")
  })
}