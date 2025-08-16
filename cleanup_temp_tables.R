# ============================================
# Databricks Temp Table Cleanup Script
# Cleans up temporary tables from OMOP Pregnancy runs
# ============================================

# CRITICAL: Set JVM parameters BEFORE loading any packages
options(java.parameters = c(
  "-Xmx4g",
  "-XX:MaxDirectMemorySize=2g",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "-Dio.netty.tryReflectionSetAccessible=true"
))

# Load required libraries
library(rJava)
.jinit()
library(DatabaseConnector)
library(dplyr)
library(DBI)

# Function to clean up temp tables
cleanup_temp_tables <- function(connection, 
                               schema = "rit_projects.preeclampsia_irb00501176",
                               pattern = "temp_",
                               dry_run = TRUE,
                               days_old = NULL) {
  
  message("========================================")
  message("Temp Table Cleanup Utility")
  message("========================================")
  message(paste("Schema:", schema))
  message(paste("Pattern:", pattern))
  message(paste("Dry run:", dry_run))
  if (!is.null(days_old)) {
    message(paste("Tables older than", days_old, "days"))
  }
  message("========================================\n")
  
  # Get list of all tables in the schema
  message("Fetching table list...")
  
  # For Databricks/Spark, use SHOW TABLES
  list_tables_sql <- paste0("SHOW TABLES IN ", schema)
  
  tables_result <- tryCatch({
    DatabaseConnector::querySql(connection, list_tables_sql)
  }, error = function(e) {
    message("Error listing tables: ", e$message)
    return(NULL)
  })
  
  if (is.null(tables_result) || nrow(tables_result) == 0) {
    message("No tables found in schema or unable to list tables.")
    return(NULL)
  }
  
  # Databricks returns columns: database, tableName, isTemporary
  # Adjust column names to lowercase if needed
  names(tables_result) <- tolower(names(tables_result))
  
  # Filter for temp tables matching pattern
  if ("tablename" %in% names(tables_result)) {
    table_col <- "tablename"
  } else if ("table_name" %in% names(tables_result)) {
    table_col <- "table_name"
  } else if ("table" %in% names(tables_result)) {
    table_col <- "table"
  } else {
    # Just use the second column if structure is different
    table_col <- names(tables_result)[2]
  }
  
  temp_tables <- tables_result %>%
    filter(grepl(pattern, !!sym(table_col), ignore.case = TRUE)) %>%
    pull(!!sym(table_col))
  
  if (length(temp_tables) == 0) {
    message("No temporary tables found matching pattern '", pattern, "'")
    return(NULL)
  }
  
  message(paste("\nFound", length(temp_tables), "temporary tables:\n"))
  
  # Create a data frame to store table info
  table_info <- data.frame(
    table_name = temp_tables,
    created_date = NA,
    size_mb = NA,
    row_count = NA,
    to_delete = TRUE,
    stringsAsFactors = FALSE
  )
  
  # Get details for each table
  for (i in seq_along(temp_tables)) {
    table_name <- temp_tables[i]
    full_table_name <- paste(schema, table_name, sep = ".")
    
    message(paste0("[", i, "/", length(temp_tables), "] Analyzing: ", table_name))
    
    # Try to get table statistics
    tryCatch({
      # Get table properties (includes creation time if available)
      desc_sql <- paste0("DESCRIBE EXTENDED ", full_table_name)
      desc_result <- DatabaseConnector::querySql(connection, desc_sql)
      
      # Try to extract creation time from properties
      if (!is.null(desc_result)) {
        names(desc_result) <- tolower(names(desc_result))
        
        # Look for Created Time in the properties
        created_row <- desc_result[grepl("Created Time|created_time|CreateTime", 
                                        desc_result[[1]], ignore.case = TRUE), ]
        if (nrow(created_row) > 0) {
          # Extract date from the value column
          date_str <- created_row[[2]][1]
          # Try to parse the date
          created_date <- tryCatch({
            as.Date(date_str)
          }, error = function(e) NA)
          
          table_info$created_date[i] <- created_date
        }
        
        # Look for statistics
        stats_row <- desc_result[grepl("Statistics|statistics", 
                                      desc_result[[1]], ignore.case = TRUE), ]
        if (nrow(stats_row) > 0) {
          stats_str <- stats_row[[2]][1]
          # Extract size if available (format: "XXX bytes")
          if (grepl("bytes", stats_str, ignore.case = TRUE)) {
            size_bytes <- as.numeric(gsub("[^0-9]", "", 
                                         strsplit(stats_str, "bytes")[[1]][1]))
            table_info$size_mb[i] <- round(size_bytes / 1024 / 1024, 2)
          }
        }
      }
      
      # Try to get row count (might fail due to Arrow issues)
      count_sql <- paste0("SELECT COUNT(*) as cnt FROM ", full_table_name)
      count_result <- tryCatch({
        DatabaseConnector::querySql(connection, count_sql)
      }, error = function(e) {
        # If counting fails, return NA
        NULL
      })
      
      if (!is.null(count_result) && nrow(count_result) > 0) {
        table_info$row_count[i] <- count_result[[1]][1]
      }
      
    }, error = function(e) {
      message(paste("  Warning: Could not get details for", table_name))
    })
    
    # Check if table should be deleted based on age
    if (!is.null(days_old) && !is.na(table_info$created_date[i])) {
      age_days <- as.numeric(Sys.Date() - table_info$created_date[i])
      if (age_days < days_old) {
        table_info$to_delete[i] <- FALSE
        message(paste("  Keeping: Table is only", age_days, "days old"))
      }
    }
  }
  
  # Display summary
  message("\n========================================")
  message("SUMMARY")
  message("========================================")
  
  # Tables to delete
  tables_to_delete <- table_info %>% filter(to_delete)
  tables_to_keep <- table_info %>% filter(!to_delete)
  
  if (nrow(tables_to_keep) > 0) {
    message(paste("\nTables to KEEP:", nrow(tables_to_keep)))
    print(tables_to_keep[, c("table_name", "created_date", "row_count")])
  }
  
  message(paste("\nTables to DELETE:", nrow(tables_to_delete)))
  if (nrow(tables_to_delete) > 0) {
    print(tables_to_delete[, c("table_name", "created_date", "size_mb", "row_count")])
    
    total_size <- sum(tables_to_delete$size_mb, na.rm = TRUE)
    if (total_size > 0) {
      message(paste("\nTotal space to reclaim:", round(total_size, 2), "MB"))
    }
  }
  
  # Execute deletion if not dry run
  if (!dry_run && nrow(tables_to_delete) > 0) {
    message("\n========================================")
    message("EXECUTING DELETION")
    message("========================================")
    
    # Confirm with user
    response <- readline("Are you sure you want to delete these tables? (yes/no): ")
    
    if (tolower(response) == "yes") {
      success_count <- 0
      fail_count <- 0
      
      for (table_name in tables_to_delete$table_name) {
        full_table_name <- paste(schema, table_name, sep = ".")
        message(paste("Dropping:", table_name))
        
        drop_sql <- paste0("DROP TABLE IF EXISTS ", full_table_name)
        
        tryCatch({
          DatabaseConnector::executeSql(connection, drop_sql)
          success_count <- success_count + 1
          message(paste("  ✓ Dropped:", table_name))
        }, error = function(e) {
          fail_count <- fail_count + 1
          message(paste("  ✗ Failed to drop:", table_name))
          message(paste("    Error:", e$message))
        })
      }
      
      message("\n========================================")
      message(paste("Deletion complete:", success_count, "succeeded,", fail_count, "failed"))
      message("========================================")
    } else {
      message("Deletion cancelled by user.")
    }
  } else if (dry_run) {
    message("\n========================================")
    message("DRY RUN COMPLETE")
    message("No tables were deleted.")
    message("Run with dry_run = FALSE to actually delete tables.")
    message("========================================")
  }
  
  return(invisible(table_info))
}

# ============================================
# MAIN EXECUTION
# ============================================

# Connection setup
message("Setting up Databricks connection...")

# Set your connection parameters
Sys.setenv(JAVA_HOME="C:/Program Files/Microsoft/jdk-17.0.11.9-hotspot")
pathToDriver <- "C:/Program Files/DatabricksJDBC42-2.6.38.1068"

# Create JDBC connection URL with Arrow disabled
jdbc_url <- paste0(
  "jdbc:databricks://adb-4068548029743470.10.azuredatabricks.net:443;",
  "httpPath=/sql/1.0/warehouses/ba264cbc36eb86d3;",
  "UseNativeQuery=0;",
  "EnableArrow=0;",
  "LowLatency=0"
)

# Create connection
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "spark",
  connectionString = jdbc_url,
  pathToDriver = pathToDriver,
  user = "token",
  password = Sys.getenv("DATABRICKS_TOKEN")
)

# Connect to database
con <- DatabaseConnector::connect(connectionDetails)

# Run cleanup
# First do a dry run to see what will be deleted
message("\n=== DRY RUN MODE ===\n")
cleanup_results <- cleanup_temp_tables(
  connection = con,
  schema = "rit_projects.preeclampsia_irb00501176",
  pattern = "temp_",
  dry_run = TRUE,  # Set to FALSE to actually delete
  days_old = NULL  # Set to a number to only delete tables older than N days
)

# To actually delete tables, uncomment and run:
# cleanup_results <- cleanup_temp_tables(
#   connection = con,
#   schema = "rit_projects.preeclampsia_irb00501176",
#   pattern = "temp_",
#   dry_run = FALSE,
#   days_old = 7  # Only delete tables older than 7 days
# )

# Disconnect
DatabaseConnector::disconnect(con)
message("\nCleanup script complete.")