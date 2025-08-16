# Test script to debug Databricks counting issue

# This script tests different counting methods to find one that works with Databricks

test_databricks_count <- function(connection, table_ref, table_name = "test_table") {
  
  cat("\n========== Testing Count Methods for", table_name, "==========\n\n")
  
  # Get dbms type
  dbms <- attr(connection, "dbms", exact = TRUE)
  cat("DBMS type:", ifelse(is.null(dbms), "NULL", dbms), "\n\n")
  
  # Method 1: Direct DatabaseConnector::querySql with simple COUNT
  cat("Method 1: Direct DatabaseConnector COUNT\n")
  tryCatch({
    # Try to get the actual table name
    if (inherits(table_ref, "tbl_sql")) {
      # Extract table name from tbl_sql object
      sql_query <- dbplyr::sql_render(table_ref %>% head(1))
      # Extract table name from the SQL (crude but might work)
      table_match <- regmatches(sql_query, regexpr("FROM\\s+([^\\s]+)", sql_query, ignore.case = TRUE))
      if (length(table_match) > 0) {
        actual_table <- gsub("^FROM\\s+", "", table_match, ignore.case = TRUE)
        count_sql <- paste0("SELECT COUNT(*) AS row_count FROM ", actual_table)
        cat("  SQL:", count_sql, "\n")
        
        result <- DatabaseConnector::querySql(connection, count_sql)
        cat("  Result columns:", paste(names(result), collapse = ", "), "\n")
        
        if (!is.null(result) && nrow(result) > 0) {
          # Try different column names
          count_val <- NULL
          if ("ROW_COUNT" %in% names(result)) {
            count_val <- result$ROW_COUNT
          } else if ("row_count" %in% names(result)) {
            count_val <- result$row_count
          } else if (ncol(result) > 0) {
            count_val <- result[[1]]
          }
          
          cat("  COUNT:", count_val, "\n")
          cat("  ✓ Method 1 succeeded!\n\n")
          return(as.numeric(count_val))
        }
      }
    }
  }, error = function(e) {
    cat("  ✗ Error:", e$message, "\n\n")
  })
  
  # Method 2: Use dbplyr to generate COUNT SQL
  cat("Method 2: dbplyr-generated COUNT\n")
  tryCatch({
    count_query <- table_ref %>%
      summarise(n = n()) %>%
      dbplyr::sql_render()
    
    cat("  SQL:", substring(as.character(count_query), 1, 200), "...\n")
    
    result <- DatabaseConnector::querySql(connection, as.character(count_query))
    cat("  Result columns:", paste(names(result), collapse = ", "), "\n")
    
    if (!is.null(result) && nrow(result) > 0) {
      count_val <- result[[1]]
      cat("  COUNT:", count_val, "\n")
      cat("  ✓ Method 2 succeeded!\n\n")
      return(as.numeric(count_val))
    }
  }, error = function(e) {
    cat("  ✗ Error:", e$message, "\n\n")
  })
  
  # Method 3: Try with DBI (likely to fail but worth testing)
  cat("Method 3: DBI::dbGetQuery\n")
  tryCatch({
    count_query <- table_ref %>%
      summarise(n = n()) %>%
      dbplyr::sql_render()
    
    result <- DBI::dbGetQuery(connection, as.character(count_query))
    if (!is.null(result) && nrow(result) > 0) {
      count_val <- result[[1]]
      cat("  COUNT:", count_val, "\n")
      cat("  ✓ Method 3 succeeded!\n\n")
      return(as.numeric(count_val))
    }
  }, error = function(e) {
    cat("  ✗ Error:", e$message, "\n\n")
  })
  
  # Method 4: Use DatabaseConnector with wrapped query
  cat("Method 4: DatabaseConnector with subquery\n")
  tryCatch({
    table_sql <- dbplyr::sql_render(table_ref)
    count_sql <- paste0("SELECT COUNT(*) AS n FROM (", as.character(table_sql), ") AS subq")
    cat("  SQL:", substring(count_sql, 1, 200), "...\n")
    
    result <- DatabaseConnector::querySql(connection, count_sql)
    cat("  Result columns:", paste(names(result), collapse = ", "), "\n")
    
    if (!is.null(result) && nrow(result) > 0) {
      count_val <- NULL
      if ("N" %in% names(result)) {
        count_val <- result$N
      } else if ("n" %in% names(result)) {
        count_val <- result$n
      } else {
        count_val <- result[[1]]
      }
      cat("  COUNT:", count_val, "\n")
      cat("  ✓ Method 4 succeeded!\n\n")
      return(as.numeric(count_val))
    }
  }, error = function(e) {
    cat("  ✗ Error:", e$message, "\n\n")
  })
  
  cat("All methods failed - unable to count\n")
  return(NA)
}

# Usage example:
# After creating HIP_concepts table:
# test_databricks_count(con, HIP_concepts, "HIP_concepts")