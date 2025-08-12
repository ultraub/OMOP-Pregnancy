# OMOPPregnancy

## Pregnancy Episode Identification in OMOP CDM Using HIPPS Algorithm

This R package implements the HIPPS (Hierarchy and rule-based pregnancy episode Inference integrated with Pregnancy Progression Signatures) algorithm for identifying pregnancy episodes in OMOP CDM databases. It enables cross-site comparison studies and benchmarking of pregnancy identification across different OMOP CDM implementations.

## Overview

The package is based on the work by Jones et al. (2023) and the All of Us implementation by Smith et al. (2024). It provides:

- **HIPPS Algorithm Implementation**: Complete implementation of the HIP, PPS, and ESD algorithms
- **Database Agnostic**: Works with any OMOP CDM v5.3+ database
- **Comparison Framework**: Tools for comparing results across different sites
- **Minimal Code Changes**: Maintains algorithm fidelity for verification and reproducibility

## Installation

```r
# Install from GitHub
devtools::install_github("yourusername/OMOPPregnancy")

# Or install from local source
devtools::install("path/to/OMOPPregnancy")
```

## Quick Start

```r
library(OMOPPregnancy)
library(DatabaseConnector)

# Create connection details for your OMOP CDM database
connectionDetails <- createConnectionDetails(
  dbms = "postgresql",
  server = "localhost/cdm",
  user = "username",
  password = "password"
)

# Run the HIPPS algorithm
results <- run_hipps(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "cdm_schema",
  resultsDatabaseSchema = "results_schema",
  outputFolder = "output/"
)

# Generate comparison report
generate_report(results, outputFolder = "output/")
```

## Database Support

The package supports all major database platforms compatible with OHDSI tools:
- PostgreSQL
- Microsoft SQL Server
- Oracle
- Amazon Redshift
- Google BigQuery
- Snowflake
- SQLite (for testing)

## Algorithm Components

### 1. HIP (Hierarchy-based Inference of Pregnancy) Algorithm
Identifies pregnancy episodes based on outcome-specific concepts and gestational timing.

### 2. PPS (Pregnancy Progression Signature) Algorithm
Uses temporal sequence analysis to detect gestational age-specific signatures.

### 3. ESD (Estimated Start Date) Algorithm
Calculates pregnancy start dates based on gestational timing information.

### 4. Episode Merging
Combines HIP and PPS episodes, removes duplicates, and assigns final outcomes.

## Comparison with All of Us

The package maintains maximum code similarity with the original All of Us implementation to enable direct comparison:

```r
# Run on All of Us
aou_results <- run_hipps(
  connectionDetails = aou_connection,
  mode = "allofus"
)

# Run on another OMOP CDM
other_results <- run_hipps(
  connectionDetails = other_connection,
  mode = "generic"
)

# Compare results
comparison <- compare_results(aou_results, other_results)
```

## Documentation

- [Getting Started](vignettes/getting_started.Rmd)
- [Algorithm Reference](docs/ALGORITHM_REFERENCE.md)
- [Architecture Guide](docs/ARCHITECTURE.md)
- [Comparison Guide](docs/COMPARISON_GUIDE.md)

## Citation

If you use this package in your research, please cite:

1. Jones SE, et al. "Who Is Pregnant? Defining Real-World Data-Based Pregnancy Episodes in the National COVID Cohort Collaborative (N3C)." JAMIA Open 2023.

2. Smith LH, et al. "Pregnancy episodes in All of Us: harnessing multi-source data for pregnancy-related research." JAMIA 2024.

## Contributing

We welcome contributions! Please see our [contributing guidelines](CONTRIBUTING.md) for details.

## License

This package is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Support

For questions, issues, or feature requests, please use the [GitHub Issues](https://github.com/yourusername/OMOPPregnancy/issues) page.