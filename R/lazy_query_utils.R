#' Utility functions for database query handling
#'
#' @name lazy_query_utils
#' @import dplyr
#' @import dbplyr
NULL

#' Check if an object is a lazy database query
#'
#' @param obj Object to check
#' @return Logical indicating if object is a lazy query
#' @export
is_lazy_query <- function(obj) {
  inherits(obj, c("tbl_lazy", "tbl_sql", "tbl_dbi"))
}

#' Smart join for database and local data
#'
#' @param left_tbl Left table (usually a database table)
#' @param right_tbl Right table (could be lazy query or local data frame)
#' @param by Column(s) to join by
#' @param type Type of join (inner, left, right, full, anti, semi)
#' @param suffix Suffixes for overlapping columns
#' @return Joined table
#' @export
smart_join <- function(left_tbl, right_tbl, by = NULL, type = "inner", suffix = c(".x", ".y")) {
  is_right_lazy <- is_lazy_query(right_tbl)
  
  join_fn <- switch(type,
    "inner" = inner_join,
    "left" = left_join,
    "right" = right_join,
    "full" = full_join,
    "anti" = anti_join,
    "semi" = semi_join,
    inner_join  # default
  )
  
  if (is_right_lazy) {
    result <- join_fn(left_tbl, right_tbl, by = by, suffix = suffix)
  } else {
    result <- join_fn(left_tbl, right_tbl, by = by, copy = TRUE, suffix = suffix)
  }
  
  return(result)
}

#' Create or get a temp table
#'
#' @param connection Database connection
#' @param data Data frame or lazy query to store
#' @param name Optional name for the temp table
#' @return Lazy reference to the temp table
#' @export
create_or_get_temp_table <- function(connection, data, name = NULL) {
  if (is_lazy_query(data)) {
    return(data)
  }
  return(create_temp_table(connection, data, name))
}

#' Collect data with row limit
#'
#' @param lazy_tbl Lazy table reference
#' @param n Maximum number of rows to collect (default 10000)
#' @param warn Whether to warn if limit is reached
#' @return Local data frame
#' @export
safe_collect <- function(lazy_tbl, n = 10000, warn = TRUE) {
  row_count <- tryCatch({
    lazy_tbl %>% 
      summarise(n = n()) %>% 
      collect() %>% 
      pull(n)
  }, error = function(e) NA)
  
  if (!is.na(row_count) && row_count > n && warn) {
    warning(paste("Table has", row_count, "rows. Collecting only first", n, "rows."))
  }
  
  result <- lazy_tbl %>%
    head(n) %>%
    collect()
  
  return(result)
}

#' Preview lazy query
#'
#' @param lazy_tbl Lazy table reference
#' @param n Number of rows to preview (default 10)
#' @param show_query Whether to show the SQL query
#' @return Prints preview and returns invisible NULL
#' @export
preview_lazy <- function(lazy_tbl, n = 10, show_query = TRUE) {
  cat("=== Table Preview ===\n")
  
  # Show dimensions if possible
  tryCatch({
    count_result <- lazy_tbl %>% 
      summarise(n = n()) %>% 
      collect()
    cat("Total rows:", count_result$n, "\n")
  }, error = function(e) {
    cat("Total rows: Unable to determine\n")
  })
  
  # Show column names
  col_names <- colnames(lazy_tbl)
  cat("Columns:", paste(col_names, collapse = ", "), "\n\n")
  
  # Show sample data
  sample_data <- lazy_tbl %>%
    head(n) %>%
    collect()
  
  print(sample_data)
  
  # Show SQL query if requested
  if (show_query) {
    cat("\n=== SQL Query ===\n")
    sql_query <- dbplyr::sql_render(lazy_tbl)
    cat(as.character(sql_query), "\n")
  }
  
  invisible(NULL)
}

#' Ensure table exists in database
#'
#' This function checks if a table needs to be uploaded to the database
#' and handles it appropriately.
#'
#' @param connection Database connection
#' @param table_or_data Table reference or local data frame
#' @param table_name Name for the table if uploading
#' @return Lazy reference to the table in database
#' @export
ensure_in_database <- function(connection, table_or_data, table_name = NULL) {
  if (is_lazy_query(table_or_data)) {
    # Already in database
    return(table_or_data)
  } else if (is.data.frame(table_or_data)) {
    # Need to upload
    if (is.null(table_name)) {
      table_name <- paste0("temp_", format(Sys.time(), "%Y%m%d_%H%M%S"))
    }
    return(create_temp_table(connection, table_or_data, table_name))
  } else {
    stop("Input must be a lazy query or data frame")
  }
}

#' Get connection from a lazy query
#'
#' Extracts the database connection from a lazy query object.
#'
#' @param lazy_tbl Lazy table reference
#' @return Database connection object
#' @export
get_connection_from_lazy <- function(lazy_tbl) {
  if (!is_lazy_query(lazy_tbl)) {
    stop("Input must be a lazy query")
  }
  
  # Try different methods to get connection
  if (!is.null(lazy_tbl$src$con)) {
    return(lazy_tbl$src$con)
  } else if (!is.null(dbplyr::remote_con(lazy_tbl))) {
    return(dbplyr::remote_con(lazy_tbl))
  } else {
    stop("Could not extract connection from lazy query")
  }
}

#' Clean up temp tables
#'
#' Removes temporary tables created during processing.
#' Important for RStudio sessions to prevent accumulation.
#'
#' @param connection Database connection
#' @param table_names Character vector of table names to drop
#' @param pattern Optional pattern to match table names
#' @export
cleanup_temp_tables <- function(connection, table_names = NULL, pattern = NULL) {
  if (!is.null(pattern)) {
    # Get all tables matching pattern
    all_tables <- DBI::dbListTables(connection)
    table_names <- grep(pattern, all_tables, value = TRUE)
  }
  
  if (!is.null(table_names)) {
    for (table_name in table_names) {
      tryCatch({
        DBI::dbRemoveTable(connection, table_name)
        message(paste("Dropped table:", table_name))
      }, error = function(e) {
        # Table might not exist or already dropped
      })
    }
  }
}