# OMOP Pregnancy Project R Profile
# This file is loaded automatically when R starts in this project directory

# JVM Configuration for Databricks Arrow Support
# These settings must be configured before any Java-using packages are loaded
# Only uncomment if you need Arrow optimization AND have proper Databricks JDBC drivers
options(java.parameters = c(
  "-Xmx8g",                                          # Increased heap for 466K+ rows
  "-XX:MaxDirectMemorySize=4g",                      # More direct memory for Arrow
  "--add-opens=java.base/java.nio=ALL-UNNAMED",      # Critical: Allow Arrow nio access
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",    # Java 17 module access
  "-Dio.netty.tryReflectionSetAccessible=true",      # Netty reflection for Arrow
  "-Dio.netty.allocator.type=unpooled"              # Avoid pooled memory issues
))

# Default JVM settings (without Arrow)
# Uncomment this block for standard Databricks connection without Arrow
#options(java.parameters = c(
#  "-Xmx4g"  # 4GB heap is usually sufficient without Arrow
#))

# Package load messages
message("========================================")
message("OMOP Pregnancy Project")
message("========================================")
# Check which JVM config is active
if (!exists(".jvm_heap_size")) {
  # Check if Arrow config is uncommented (line 7 not commented)
  .jvm_heap_size <- "8GB (Arrow-optimized)"
  # This is a simplification - in practice the active config is on lines 7-14
}
message("JVM heap size: 8GB (Arrow-optimized)")
message("Arrow optimization: Configured in JVM settings")
message("To disable Arrow: ")
message("  1. Comment out lines 7-14 (Arrow JVM settings)")
message("  2. Uncomment lines 18-20 (Standard JVM settings)")
message("  3. Set ENABLE_ARROW=FALSE in .env")
message("  4. Restart R session")
message("========================================\n")

# Load .env file automatically if it exists
if (file.exists(".env")) {
  # Simple .env loader
  env_lines <- readLines(".env", warn = FALSE)
  for (line in env_lines) {
    line <- trimws(line)
    if (nchar(line) > 0 && !startsWith(line, "#")) {
      parts <- strsplit(line, "=", fixed = TRUE)[[1]]
      if (length(parts) >= 2) {
        key <- trimws(parts[1])
        value <- trimws(paste(parts[-1], collapse = "="))
        value <- gsub("^['\"]|['\"]$", "", value)
        do.call(Sys.setenv, setNames(list(value), key))
      }
    }
  }
  message("✓ Loaded environment variables from .env\n")
}

# Set default options for better display
options(
  width = 120,                    # Wider console output
  scipen = 999,                    # Avoid scientific notation
  stringsAsFactors = FALSE,       # Modern R default
  max.print = 1000,                # Limit console output
  warn = 1                         # Show warnings immediately
)

# Databricks/Spark specific options
options(
  dbplyr.compute.defaults = list(temporary = FALSE),  # No temp tables with #
  dbplyr.temp_prefix = "temp_"                        # Use temp_ prefix instead
)

# Helper function to test connection
.test_connection <- function() {
  message("Testing database connection...")
  tryCatch({
    source("R/00_connection/create_connection.R")
    con <- create_connection_from_env()
    message("✓ Connection successful!")
    return(con)
  }, error = function(e) {
    message("✗ Connection failed: ", e$message)
    return(NULL)
  })
}

# Remind user about setup
if (!dir.exists("jdbc_drivers") || length(list.files("jdbc_drivers", pattern = "\\.jar$")) == 0) {
  message("⚠ Warning: JDBC drivers not found")
  message("  Run: source('inst/scripts/setup_jdbc_drivers.R')")
  message("")
}

message("Ready! Use .test_connection() to test your database connection.\n")