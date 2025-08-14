# Test database connection with provided credentials
library(DBI)
library(odbc)

cat("Testing database connection...\n")

# Try different connection methods
# Method 1: Using DBI with odbc
tryCatch({
  con <- dbConnect(
    odbc::odbc(),
    Driver = "ODBC Driver 17 for SQL Server",
    Server = "ohdsicdmsqlserver.database.windows.net",
    Database = "tufts",
    UID = "dbadmin",
    PWD = "",
    Port = 1433
  )
  cat("✓ Connected via odbc\n")
  
  # Test query
  tables <- dbListTables(con)
  cat("Tables found:", length(tables), "\n")
  if (length(tables) > 0) {
    cat("First few tables:", head(tables), "\n")
  }
  
  dbDisconnect(con)
}, error = function(e) {
  cat("✗ ODBC connection failed:", e$message, "\n")
})

# Method 2: Test network connectivity
cat("\nTesting network connectivity...\n")
system("ping -c 1 ohdsicdmsqlserver.database.windows.net", intern = FALSE)
