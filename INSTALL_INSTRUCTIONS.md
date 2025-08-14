# OMOPPregnancy Package Installation Instructions

## Package Details
- **Version**: 0.1.0
- **Size**: 8.2 MB
- **MD5**: 28411bc0928bdbadd6418f19f230b7e6

## Prerequisites

### 1. Java Installation (Required for full functionality)
- Java 8 or higher required
- Set JAVA_HOME environment variable:
  - Windows: `set JAVA_HOME=C:\Program Files\Java\jdk-17`
  - Mac: `export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home`
  - Linux: `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk`

### 2. Required R Packages
Install dependencies first:
```r
install.packages(c(
  "DBI", "dplyr", "dbplyr", "tidyr", "lubridate",
  "purrr", "readr", "readxl", "yaml", "stringr", "here"
))

# Java-dependent packages (require Java setup)
install.packages("DatabaseConnector")
install.packages("SqlRender")
```

## Installation

### From Local File
```r
# Install from the tar.gz file
install.packages("OMOPPregnancy_0.1.0.tar.gz", 
                 repos = NULL, 
                 type = "source")
```

### Load the Package
```r
library(OMOPPregnancy)
```

## Quick Test
```r
# Test configuration loading
config <- load_config(mode = "generic")

# Check available functions
ls("package:OMOPPregnancy")
```

## Troubleshooting

### Java/DatabaseConnector Issues
If you get Java-related errors:
1. Ensure Java is installed: `java -version`
2. Set JAVA_HOME correctly
3. Restart R after setting JAVA_HOME
4. Try: `DatabaseConnector::downloadJdbcDrivers("sql server")`

### Namespace Conflicts
The package carefully manages namespace conflicts. If you see warnings about replaced imports, they are expected and handled.

### Windows UNC Path Issue
If installing from a network drive on Windows, copy the file to a local directory first:
```r
# Copy to local temp directory
file.copy("\\\\network\\path\\OMOPPregnancy_0.1.0.tar.gz", 
          "C:/temp/OMOPPregnancy_0.1.0.tar.gz")

# Install from local copy
install.packages("C:/temp/OMOPPregnancy_0.1.0.tar.gz", 
                 repos = NULL, 
                 type = "source")
```

## Verification
After installation, verify the package works:
```r
# Should return version 0.1.0
packageVersion("OMOPPregnancy")

# Test main function exists
exists("run_hipps", where = asNamespace("OMOPPregnancy"))
```

## Support
For issues or questions, please refer to the package documentation or create an issue in the repository.