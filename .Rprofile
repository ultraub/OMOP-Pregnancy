# OMOP Pregnancy Project R Profile
# This file is loaded automatically when R starts in this project directory

# JVM configuration (must be set before any Java-using package loads)
#
# Default: a 4 GB heap, no Arrow. This is enough for standard JDBC use on
# every supported platform and matches ENABLE_ARROW=FALSE in .env.
options(java.parameters = c("-Xmx4g"))

# Optional: Databricks Arrow transfer. Requires the full Databricks JDBC
# driver with its Arrow dependencies, ENABLE_ARROW=TRUE in .env, and enough
# memory. To use it, comment out the line above and uncomment this block.
# options(java.parameters = c(
#   "-Xmx8g",
#   "-XX:MaxDirectMemorySize=4g",
#   "--add-opens=java.base/java.nio=ALL-UNNAMED",
#   "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
#   "-Dio.netty.tryReflectionSetAccessible=true",
#   "-Dio.netty.allocator.type=unpooled"
# ))

message("========================================")
message("OMOP Pregnancy Project")
message("========================================")
message("JVM options: ", paste(getOption("java.parameters"), collapse = " "))
message("(edit .Rprofile to switch between the standard and Arrow JVM settings)")
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
        # Avoid setNames which may not be loaded yet
        env_list <- list(value)
        names(env_list) <- key
        do.call(Sys.setenv, env_list)
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
# TODO - Update this to check for .env
if (!dir.exists("jdbc_drivers") || length(list.files("jdbc_drivers", pattern = "\\.jar$")) == 0) {
  message("⚠ Warning: JDBC drivers not found")
  message("  Run: source('inst/scripts/setup_jdbc_drivers.R')")
  message("")
}

message("Ready! Use .test_connection() to test your database connection.\n")
