# Quick Start Guide for OMOP Pregnancy Package

## 1. One-Line Installation (if prerequisites are met)

```r
source("https://raw.githubusercontent.com/ultraub/OMOP-Pregnancy/main/install_dependencies.R")
```

## 2. Basic Installation Steps

```r
# Step 1: Install package manager
install.packages("remotes")

# Step 2: Install the package and dependencies
remotes::install_github("ultraub/OMOP-Pregnancy")

# Or if you have it locally:
remotes::install_local(".", dependencies = TRUE)
```

## 3. Quick Setup

```r
# Load the package
library(OMOPPregnancy)

# Setup environment (copy .Renv.example to .Renv first and add your credentials)
load_env()

# Get database connection
connectionDetails <- get_connection_from_env()
connection <- connect(connectionDetails)

# Set connection attributes
attr(connection, 'mode') <- 'generic'
attr(connection, 'dbms') <- 'sql server'  # or your database type
attr(connection, 'cdmDatabaseSchema') <- 'dbo'  # or your schema
```

## 4. Run the Algorithm

```r
# Load concept sets
HIP_concepts <- readxl::read_excel(system.file("extdata", "HIP_concepts.xlsx", package = "OMOPPregnancy"))
PPS_concepts <- readxl::read_excel(system.file("extdata", "PPS_concepts.xlsx", package = "OMOPPregnancy"))

# Get OMOP CDM tables
person_tbl <- get_cdm_table(connection, 'person')
condition_occurrence_tbl <- get_cdm_table(connection, 'condition_occurrence')
procedure_occurrence_tbl <- get_cdm_table(connection, 'procedure_occurrence')
observation_tbl <- get_cdm_table(connection, 'observation')
measurement_tbl <- get_cdm_table(connection, 'measurement')
visit_occurrence_tbl <- get_cdm_table(connection, 'visit_occurrence')

# Run HIP algorithm
hip_episodes <- get_outcome_episodes(
  condition_occurrence_tbl,
  procedure_occurrence_tbl,
  observation_tbl,
  measurement_tbl,
  visit_occurrence_tbl,
  person_tbl,
  HIP_concepts,
  config = NULL  # Uses defaults
)

# Run PPS algorithm
pps_input <- input_GT_concepts(
  condition_occurrence_tbl,
  procedure_occurrence_tbl,
  observation_tbl,
  measurement_tbl,
  visit_occurrence_tbl,
  PPS_concepts
)

pps_episodes <- get_PPS_episodes(
  pps_input,
  PPS_concepts,
  person_tbl,
  config = NULL
)

# Clean up
disconnect(connection)
```

## 5. Using Docker (Alternative)

```dockerfile
FROM rocker/r-ver:4.3.0

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    libxml2-dev \
    libcurl4-openssl-dev \
    libssl-dev

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install R packages
RUN R -e "install.packages('remotes')"
RUN R -e "remotes::install_github('ultraub/OMOP-Pregnancy')"

# Copy your .Renv file
COPY .Renv /workspace/.Renv
WORKDIR /workspace

CMD ["R"]
```

## Common Issues & Solutions

### Issue: Java not found
```r
# Set Java path before loading DatabaseConnector
Sys.setenv(JAVA_HOME = "/path/to/java")
```

### Issue: Package not found
```r
# Install from source
install.packages("package_name", repos = "https://cloud.r-project.org/")
```

### Issue: Database connection fails
```r
# Check your .Renv file has correct credentials
# Test connection
DatabaseConnector::testConnection(connectionDetails)
```

## Next Steps
- Read full documentation: [README.md](README.md)
- Review installation details: [README_INSTALLATION.md](README_INSTALLATION.md)
- Check algorithm details: [TECHNICAL_DOCUMENTATION.md](TECHNICAL_DOCUMENTATION.md)
- Report issues: https://github.com/ultraub/OMOP-Pregnancy/issues