#' Connection compatibility fixes for DatabaseConnector
#'
#' Provides missing S3 methods for DatabaseConnectorJdbcConnection
#' to ensure compatibility with dbplyr operations.

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
  if (requireNamespace("dbplyr", quietly = TRUE) && 
      requireNamespace("DatabaseConnector", quietly = TRUE)) {
    registerS3method("sql_join_suffix", "DatabaseConnectorJdbcConnection", 
                     sql_join_suffix.DatabaseConnectorJdbcConnection,
                     envir = asNamespace("dbplyr"))
  }
}

# Fallback registration if .onLoad doesn't execute
if (!exists("sql_join_suffix.DatabaseConnectorJdbcConnection")) {
  sql_join_suffix.DatabaseConnectorJdbcConnection <- function(con, suffix = c(".x", ".y")) {
    suffix
  }
  
  tryCatch({
    registerS3method("sql_join_suffix", "DatabaseConnectorJdbcConnection", 
                     sql_join_suffix.DatabaseConnectorJdbcConnection,
                     envir = asNamespace("dbplyr"))
  }, error = function(e) {
    message("Note: Could not register sql_join_suffix method, but function is available")
  })
}