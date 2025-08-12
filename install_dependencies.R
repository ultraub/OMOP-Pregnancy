#!/usr/bin/env Rscript

#' Install Dependencies for OMOP Pregnancy Package
#' 
#' This script installs all required dependencies for the OMOP Pregnancy package.
#' It handles both CRAN and GitHub packages, including OHDSI-specific packages.
#'
#' Usage:
#'   Rscript install_dependencies.R
#'   
#' Or from R console:
#'   source("install_dependencies.R")

cat("========================================\n")
cat("OMOP Pregnancy Package Dependency Installer\n")
cat("========================================\n\n")

# Function to check and install packages
install_if_missing <- function(package, repo = "CRAN") {
  if (!requireNamespace(package, quietly = TRUE)) {
    cat(sprintf("Installing %s from %s...\n", package, repo))
    if (repo == "CRAN") {
      install.packages(package, repos = "https://cloud.r-project.org/")
    }
    return(TRUE)
  } else {
    cat(sprintf("✓ %s already installed\n", package))
    return(FALSE)
  }
}

# Check for remotes package (needed for GitHub installations)
if (!requireNamespace("remotes", quietly = TRUE)) {
  cat("Installing remotes package for GitHub installations...\n")
  install.packages("remotes", repos = "https://cloud.r-project.org/")
}

# CRAN packages (from Imports in DESCRIPTION)
cran_packages <- c(
  "DBI",
  "dplyr", 
  "dbplyr",
  "tidyr",
  "readr",
  "lubridate",
  "rlang",
  "tibble",
  "purrr",
  "here",
  "readxl",
  "yaml"
)

# Suggested packages for development and testing
suggested_packages <- c(
  "testthat",
  "knitr",
  "rmarkdown",
  "covr"
)

cat("Installing required CRAN packages...\n")
cat("=====================================\n")
installed_count <- 0
for (pkg in cran_packages) {
  if (install_if_missing(pkg)) {
    installed_count <- installed_count + 1
  }
}
cat(sprintf("\n%d CRAN packages newly installed\n\n", installed_count))

# OHDSI packages from GitHub
cat("Installing OHDSI packages...\n")
cat("=====================================\n")

# Check Java configuration first
check_java <- function() {
  java_home <- Sys.getenv("JAVA_HOME")
  if (java_home == "") {
    cat("\n⚠️  WARNING: JAVA_HOME not set\n")
    cat("DatabaseConnector requires Java. Please set JAVA_HOME:\n")
    
    # Provide OS-specific instructions
    os_type <- Sys.info()["sysname"]
    if (os_type == "Darwin") {  # macOS
      cat("  macOS: Install Java with: brew install openjdk@17\n")
      cat("  Then set: Sys.setenv(JAVA_HOME='/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home')\n")
    } else if (os_type == "Linux") {
      cat("  Linux: Install Java with: sudo apt-get install openjdk-17-jdk\n")
      cat("  Then set: Sys.setenv(JAVA_HOME='/usr/lib/jvm/java-17-openjdk')\n")
    } else if (os_type == "Windows") {
      cat("  Windows: Download Java from https://adoptium.net/\n")
      cat("  Then set: Sys.setenv(JAVA_HOME='C:/Program Files/Java/jdk-17')\n")
    }
    cat("\n")
    return(FALSE)
  } else {
    cat(sprintf("✓ JAVA_HOME is set to: %s\n", java_home))
    return(TRUE)
  }
}

java_ok <- check_java()

# Install OHDSI packages
ohdsi_packages <- list(
  DatabaseConnector = "OHDSI/DatabaseConnector",
  SqlRender = "OHDSI/SqlRender",
  CDMConnector = "darwin-eu-dev/CDMConnector",
  Eunomia = "OHDSI/Eunomia"
)

for (pkg_name in names(ohdsi_packages)) {
  if (!requireNamespace(pkg_name, quietly = TRUE)) {
    cat(sprintf("Installing %s from GitHub...\n", pkg_name))
    tryCatch({
      remotes::install_github(ohdsi_packages[[pkg_name]], quiet = FALSE)
      cat(sprintf("✓ %s installed successfully\n", pkg_name))
    }, error = function(e) {
      cat(sprintf("✗ Failed to install %s: %s\n", pkg_name, e$message))
      if (pkg_name == "DatabaseConnector" && !java_ok) {
        cat("  This is likely due to missing Java configuration\n")
      }
    })
  } else {
    cat(sprintf("✓ %s already installed\n", pkg_name))
  }
}

# Download JDBC drivers for DatabaseConnector
cat("\nDownloading JDBC drivers...\n")
cat("=====================================\n")
if (requireNamespace("DatabaseConnector", quietly = TRUE)) {
  # Create directory for JDBC drivers
  jdbc_folder <- "inst/jdbc"
  if (!dir.exists(jdbc_folder)) {
    dir.create(jdbc_folder, recursive = TRUE)
    cat(sprintf("Created JDBC folder: %s\n", jdbc_folder))
  }
  
  # Set the JAR folder
  Sys.setenv(DATABASECONNECTOR_JAR_FOLDER = jdbc_folder)
  
  # Download drivers for common databases
  drivers_to_download <- c("sql server", "postgresql", "oracle", "redshift")
  
  for (driver in drivers_to_download) {
    cat(sprintf("Downloading %s driver...\n", driver))
    tryCatch({
      DatabaseConnector::downloadJdbcDrivers(driver, pathToDriver = jdbc_folder)
      cat(sprintf("✓ %s driver downloaded\n", driver))
    }, error = function(e) {
      cat(sprintf("✗ Could not download %s driver: %s\n", driver, e$message))
    })
  }
} else {
  cat("⚠️  DatabaseConnector not installed, skipping JDBC driver download\n")
}

# Optional: Install suggested packages
cat("\nWould you like to install suggested packages for development? (y/n): ")
response <- readline()
if (tolower(response) %in% c("y", "yes")) {
  cat("\nInstalling suggested packages...\n")
  cat("=====================================\n")
  for (pkg in suggested_packages) {
    install_if_missing(pkg)
  }
}

# Verify installation
cat("\n========================================\n")
cat("Verifying installation...\n")
cat("========================================\n")

all_required <- c(cran_packages, names(ohdsi_packages)[1:2])  # DatabaseConnector and SqlRender are required
missing_packages <- c()

for (pkg in all_required) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    missing_packages <- c(missing_packages, pkg)
  }
}

if (length(missing_packages) == 0) {
  cat("\n✅ All required packages are installed successfully!\n")
  cat("\nYou can now install the OMOP Pregnancy package with:\n")
  cat("  devtools::install() or remotes::install_local()\n")
} else {
  cat("\n⚠️  The following required packages are missing:\n")
  for (pkg in missing_packages) {
    cat(sprintf("  - %s\n", pkg))
  }
  cat("\nPlease install these packages manually or re-run this script.\n")
}

cat("\n========================================\n")
cat("Installation complete!\n")
cat("========================================\n")