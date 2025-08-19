#!/usr/bin/env Rscript

# ============================================
# OMOP Pregnancy V2 - Setup Script
# Prepares environment for analysis
# ============================================

cat("========================================\n")
cat("OMOP Pregnancy V2 Setup\n")
cat("========================================\n\n")

# ============================================
# INSTALL REQUIRED PACKAGES
# ============================================

cat("Checking and installing required packages...\n")

required_packages <- c(
  "DatabaseConnector",
  "SqlRender",
  "dplyr",
  "lubridate",
  "readr",
  "tibble",
  "tidyr",
  "DBI"
)

# Check which packages need installation
missing_packages <- required_packages[!required_packages %in% installed.packages()[,"Package"]]

if (length(missing_packages) > 0) {
  cat("Installing missing packages:\n")
  for (pkg in missing_packages) {
    cat(sprintf("  - Installing %s...\n", pkg))
    install.packages(pkg, quiet = TRUE)
  }
} else {
  cat("  ✓ All required packages are installed\n")
}

# ============================================
# DOWNLOAD JDBC DRIVERS
# ============================================

cat("\n========================================\n")
cat("JDBC Driver Setup\n")
cat("========================================\n")

jdbc_dir <- "jdbc_drivers"

if (!dir.exists(jdbc_dir)) {
  dir.create(jdbc_dir)
  cat(sprintf("Created directory: %s\n", jdbc_dir))
}

# Function to download JDBC driver
download_jdbc <- function(dbms, jdbc_dir) {
  if (dbms == "sql server") {
    # SQL Server JDBC driver
    if (!file.exists(file.path(jdbc_dir, "mssql-jdbc-12.2.0.jre11.jar"))) {
      cat("\nDownloading SQL Server JDBC driver...\n")
      DatabaseConnector::downloadJdbcDrivers("sql server", pathToDriver = jdbc_dir)
      cat("  ✓ SQL Server JDBC driver downloaded\n")
    } else {
      cat("  ✓ SQL Server JDBC driver already exists\n")
    }
  } else if (dbms == "spark") {
    cat("\nDatabricks JDBC driver must be downloaded manually:\n")
    cat("  1. Go to: https://www.databricks.com/spark/jdbc-drivers-download\n")
    cat("  2. Download the JDBC driver\n")
    cat("  3. Extract to: ", jdbc_dir, "\n")
    cat("  4. Update DATABRICKS_JDBC_PATH in your .env file\n")
  }
}

# Prompt user for environment
cat("\nWhich environment will you be using?\n")
cat("  1. SQL Server\n")
cat("  2. Databricks\n")
cat("  3. Both\n")
cat("Enter choice (1-3): ")

choice <- readline()

if (choice %in% c("1", "3")) {
  download_jdbc("sql server", jdbc_dir)
}

if (choice %in% c("2", "3")) {
  download_jdbc("spark", jdbc_dir)
}

# ============================================
# CREATE ENVIRONMENT FILE
# ============================================

cat("\n========================================\n")
cat("Environment Configuration\n")
cat("========================================\n")

if (!file.exists(".env")) {
  cat("\nNo .env file found. Would you like to create one?\n")
  cat("  1. Yes, copy the template\n")
  cat("  2. No, skip\n")
  cat("Enter choice (1-2): ")
  
  env_choice <- readline()
  
  if (env_choice == "1") {
    # Copy unified template
    if (file.exists("inst/templates/.env.template")) {
      file.copy("inst/templates/.env.template", ".env")
      cat("\n✓ Created .env file from template\n")
      cat("  Please edit .env and:\n")
      cat("    1. Set DB_TYPE to your database platform (sql server, databricks, etc.)\n")
      cat("    2. Fill in your connection details\n")
      cat("    3. Configure schema names\n")
    } else {
      cat("\n✗ Template file not found at inst/templates/.env.template\n")
    }
  }
} else {
  cat("\n✓ .env file already exists\n")
}

# ============================================
# CREATE OUTPUT DIRECTORY
# ============================================

if (!dir.exists("output")) {
  dir.create("output")
  cat("\n✓ Created output directory\n")
}

# ============================================
# VERIFY R VERSION
# ============================================

cat("\n========================================\n")
cat("System Information\n")
cat("========================================\n")

r_version <- R.version.string
cat(sprintf("R Version: %s\n", r_version))

# Check R version (should be 4.0+)
if (as.numeric(R.version$major) < 4) {
  cat("⚠ Warning: R version 4.0+ recommended\n")
}

# Check Java
java_home <- Sys.getenv("JAVA_HOME")
if (java_home == "") {
  cat("⚠ JAVA_HOME not set\n")
  cat("  For Databricks, ensure Java 8+ is installed\n")
} else {
  cat(sprintf("JAVA_HOME: %s\n", java_home))
}

# ============================================
# TEST LOADING COMPONENTS
# ============================================

cat("\n========================================\n")
cat("Testing Component Loading\n")
cat("========================================\n")

test_load <- function(path) {
  if (dir.exists(path)) {
    files <- list.files(path, pattern = "\\.R$", recursive = TRUE)
    cat(sprintf("  ✓ Found %d R files in %s\n", length(files), path))
    return(TRUE)
  } else {
    cat(sprintf("  ✗ Directory not found: %s\n", path))
    return(FALSE)
  }
}

components <- c(
  "R/00_concepts",
  "R/01_extraction", 
  "R/02_algorithms",
  "R/03_utilities",
  "R/03_results"
)

all_found <- TRUE
for (comp in components) {
  if (!test_load(comp)) {
    all_found <- FALSE
  }
}

if (all_found) {
  cat("\n✓ All components found\n")
} else {
  cat("\n⚠ Some components missing. Check your installation.\n")
}

# ============================================
# FINAL INSTRUCTIONS
# ============================================

cat("\n========================================\n")
cat("Setup Complete!\n")
cat("========================================\n\n")

cat("Next steps:\n")
cat("  1. Edit .env file with your connection details\n")
cat("  2. Ensure JDBC drivers are in place\n")
cat("  3. Run the analysis:\n")
cat("     Rscript inst/scripts/run_pregnancy_analysis.R\n")
cat("\nFor help, see the README.md file\n")

cat("\n✅ Setup complete!\n")