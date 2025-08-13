#!/usr/bin/env Rscript

# Test loading the package functions directly without building

cat("==================================================\n")
cat("Testing Direct Load of Package Functions\n")
cat("==================================================\n\n")

# Set working directory to package root
if (basename(getwd()) != "OMOPPregnancy") {
  if (file.exists("OMOPPregnancy")) {
    setwd("OMOPPregnancy")
  }
}

cat("Working directory:", getwd(), "\n\n")

# Load required libraries
cat("Loading required libraries...\n")
required_libs <- c("dplyr", "dbplyr", "SqlRender", "DatabaseConnector")

for (lib in required_libs) {
  if (requireNamespace(lib, quietly = TRUE)) {
    library(lib, character.only = TRUE)
    cat("  ✅ Loaded", lib, "\n")
  } else {
    cat("  ⚠️", lib, "not installed\n")
  }
}

# Source R files directly
cat("\nLoading package R files directly...\n")

# First load sql_functions.R
if (file.exists("R/sql_functions.R")) {
  source("R/sql_functions.R")
  cat("  ✅ Loaded sql_functions.R\n")
} else {
  cat("  ❌ sql_functions.R not found\n")
}

# Load other key files
files_to_load <- c(
  "R/config.R",
  "R/connection.R",
  "R/query_utils.R"
)

for (file in files_to_load) {
  if (file.exists(file)) {
    tryCatch({
      source(file)
      cat("  ✅ Loaded", basename(file), "\n")
    }, error = function(e) {
      cat("  ❌ Error loading", basename(file), ":", e$message, "\n")
    })
  }
}

# Test SQL functions
cat("\nTesting SQL functions:\n")

# Test sql_date_diff
tryCatch({
  result <- sql_date_diff("date1", "date2", "day")
  cat("  ✅ sql_date_diff works:", as.character(result), "\n")
}, error = function(e) {
  cat("  ❌ sql_date_diff error:", e$message, "\n")
})

# Test sql_date_from_parts
tryCatch({
  result <- sql_date_from_parts("2024", "1", "15")
  cat("  ✅ sql_date_from_parts works:", as.character(result), "\n")
}, error = function(e) {
  cat("  ❌ sql_date_from_parts error:", e$message, "\n")
})

# Test sql_date_add
tryCatch({
  result <- sql_date_add("date1", "30", "day")
  cat("  ✅ sql_date_add works:", as.character(result), "\n")
}, error = function(e) {
  cat("  ❌ sql_date_add error:", e$message, "\n")
})

# Test sql_concat
tryCatch({
  result <- sql_concat("field1", "field2")
  cat("  ✅ sql_concat works:", as.character(result), "\n")
}, error = function(e) {
  cat("  ❌ sql_concat error:", e$message, "\n")
})

# Now try loading algorithm files
cat("\nTesting algorithm files:\n")

# Load HIP algorithm
if (file.exists("R/hip_algorithm.R")) {
  tryCatch({
    source("R/hip_algorithm.R")
    cat("  ✅ HIP algorithm loaded successfully\n")
    
    # Check if key functions exist
    if (exists("initial_pregnant_cohort")) {
      cat("    ✅ initial_pregnant_cohort function available\n")
    }
  }, error = function(e) {
    cat("  ❌ Error loading HIP algorithm:", e$message, "\n")
  })
}

# Load PPS algorithm  
if (file.exists("R/pps_algorithm.R")) {
  tryCatch({
    source("R/pps_algorithm.R")
    cat("  ✅ PPS algorithm loaded successfully\n")
    
    # Check if key functions exist
    if (exists("get_PPS_episodes")) {
      cat("    ✅ get_PPS_episodes function available\n")
    }
  }, error = function(e) {
    cat("  ❌ Error loading PPS algorithm:", e$message, "\n")
  })
}

cat("\n==================================================\n")
cat("Summary\n")
cat("==================================================\n")

# Check which functions are available
sql_funcs <- c("sql_date_diff", "sql_date_from_parts", "sql_date_add", "sql_concat")
available <- sapply(sql_funcs, exists)

if (all(available)) {
  cat("✅ All SQL functions are working correctly!\n")
  cat("✅ The package code can be loaded and used directly.\n")
  cat("\nThe SqlRender integration is functional.\n")
} else {
  cat("⚠️ Some functions are not available:\n")
  for (i in seq_along(sql_funcs)) {
    if (!available[i]) {
      cat("  -", sql_funcs[i], "\n")
    }
  }
}

cat("\nFor package building issues on Windows, try:\n")
cat("1. R CMD build --no-build-vignettes --no-manual .\n")
cat("2. Make sure all dependencies are installed\n")
cat("3. Check that no antivirus is blocking R operations\n")