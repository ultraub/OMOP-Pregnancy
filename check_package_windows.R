#!/usr/bin/env Rscript

# Diagnostic script for Windows package build issues

cat("==================================================\n")
cat("Package Build Diagnostics for Windows\n")
cat("==================================================\n\n")

# Check R version and platform
cat("R Version:\n")
print(R.version.string)
cat("\nPlatform:\n")
print(R.version$platform)
cat("\n")

# Check if we're on Windows
is_windows <- .Platform$OS.type == "windows"
cat("Running on Windows:", is_windows, "\n\n")

# Check working directory
cat("Working directory:\n")
print(getwd())
cat("\n")

# Check if DESCRIPTION file exists and is readable
cat("Checking DESCRIPTION file:\n")
if (file.exists("DESCRIPTION")) {
  cat("  ✅ DESCRIPTION file exists\n")
  
  # Read and check for issues
  desc_lines <- readLines("DESCRIPTION", warn = FALSE)
  cat("  Lines in DESCRIPTION:", length(desc_lines), "\n")
  
  # Check for incomplete last line
  last_line <- tail(desc_lines, 1)
  if (nchar(last_line) > 0 && !grepl("\\n$", last_line)) {
    cat("  ⚠️  Warning: DESCRIPTION file may be missing final newline\n")
  }
  
  # Check for Windows line endings
  desc_raw <- readBin("DESCRIPTION", "raw", file.info("DESCRIPTION")$size)
  has_crlf <- any(desc_raw == as.raw(0x0d))
  if (has_crlf) {
    cat("  ℹ️  DESCRIPTION has Windows (CRLF) line endings\n")
  } else {
    cat("  ℹ️  DESCRIPTION has Unix (LF) line endings\n")
  }
} else {
  cat("  ❌ DESCRIPTION file not found!\n")
}

# Check NAMESPACE file
cat("\nChecking NAMESPACE file:\n")
if (file.exists("NAMESPACE")) {
  cat("  ✅ NAMESPACE file exists\n")
  
  # Try to parse it
  tryCatch({
    ns_content <- readLines("NAMESPACE", warn = FALSE)
    cat("  Lines in NAMESPACE:", length(ns_content), "\n")
    
    # Check for common issues
    import_lines <- grep("^import\\(", ns_content, value = TRUE)
    export_lines <- grep("^export\\(", ns_content, value = TRUE)
    cat("  Import statements:", length(import_lines), "\n")
    cat("  Export statements:", length(export_lines), "\n")
    
    # Check for duplicate exports
    exports <- gsub("export\\((.*)\\)", "\\1", export_lines)
    if (any(duplicated(exports))) {
      cat("  ⚠️  Warning: Duplicate exports found:\n")
      print(exports[duplicated(exports)])
    }
  }, error = function(e) {
    cat("  ❌ Error reading NAMESPACE:", e$message, "\n")
  })
} else {
  cat("  ❌ NAMESPACE file not found!\n")
}

# Check R directory
cat("\nChecking R directory:\n")
if (dir.exists("R")) {
  r_files <- list.files("R", pattern = "\\.R$", full.names = FALSE)
  cat("  R files found:", length(r_files), "\n")
  
  # Check for backup files
  backup_files <- list.files("R", pattern = "\\.backup$", full.names = FALSE)
  if (length(backup_files) > 0) {
    cat("  ⚠️  Warning: Backup files found in R/:\n")
    for (f in backup_files) {
      cat("    -", f, "\n")
    }
  }
  
  # Check if sql_functions.R exists
  if ("sql_functions.R" %in% r_files) {
    cat("  ✅ sql_functions.R exists\n")
  } else {
    cat("  ❌ sql_functions.R not found!\n")
  }
} else {
  cat("  ❌ R directory not found!\n")
}

# Try to load individual files to check for syntax errors
cat("\nChecking R files for syntax errors:\n")
r_files <- list.files("R", pattern = "\\.R$", full.names = TRUE)
for (file in r_files) {
  cat("  Checking", basename(file), "... ")
  tryCatch({
    parse(file)
    cat("✅ OK\n")
  }, error = function(e) {
    cat("❌ SYNTAX ERROR\n")
    cat("    Error:", e$message, "\n")
  })
}

# Check if required packages are installed
cat("\nChecking required packages:\n")
required_packages <- c("DBI", "dplyr", "dbplyr", "SqlRender", "DatabaseConnector")
for (pkg in required_packages) {
  if (requireNamespace(pkg, quietly = TRUE)) {
    cat("  ✅", pkg, "is installed\n")
  } else {
    cat("  ❌", pkg, "is NOT installed\n")
  }
}

# Try a minimal build without devtools
cat("\nTrying basic R CMD check structure:\n")
tryCatch({
  # Check if package structure is valid
  pkg_structure_ok <- all(
    file.exists("DESCRIPTION"),
    file.exists("NAMESPACE"),
    dir.exists("R")
  )
  
  if (pkg_structure_ok) {
    cat("  ✅ Basic package structure is valid\n")
  } else {
    cat("  ❌ Package structure is incomplete\n")
  }
}, error = function(e) {
  cat("  ❌ Error checking structure:", e$message, "\n")
})

cat("\n==================================================\n")
cat("Diagnostic Summary\n")
cat("==================================================\n")
cat("\nIf you're seeing build errors, try:\n")
cat("1. Ensure all required packages are installed\n")
cat("2. Remove any backup files from R/ directory\n")
cat("3. Check that DESCRIPTION has a final newline\n")
cat("4. Try building with: R CMD build --no-build-vignettes .\n")
cat("5. On Windows, use forward slashes in file paths\n")