library(DatabaseConnector)
library(dplyr)

# Load environment
if (file.exists(".env")) {
  env_lines <- readLines(".env")
  for (line in env_lines) {
    line <- trimws(line)
    if (nchar(line) > 0 && !startsWith(line, "#")) {
      parts <- strsplit(line, "=", fixed = TRUE)[[1]]
      if (length(parts) >= 2) {
        key <- trimws(parts[1])
        value <- trimws(paste(parts[-1], collapse = "="))
        do.call(Sys.setenv, setNames(list(value), key))
      }
    }
  }
}

# Connection setup
server <- Sys.getenv("DB_SERVER")
database <- Sys.getenv("DB_DATABASE")
user <- Sys.getenv("DB_USER")
password <- Sys.getenv("DB_PASSWORD")
port <- as.numeric(Sys.getenv("DB_PORT", "1433"))
cdm_schema <- Sys.getenv("CDM_SCHEMA", "dbo")

jdbc_folder <- "jdbc_drivers"
jdbc_url <- sprintf(
  "jdbc:sqlserver://%s:%d;database=%s;encrypt=true;trustServerCertificate=true;",
  server, port, database
)

connectionDetails <- createConnectionDetails(
  dbms = "sql server",
  connectionString = jdbc_url,
  user = user,
  password = password,
  pathToDriver = jdbc_folder
)

connection <- connect(connectionDetails)

# Test query to see how dates come through
test_sql <- "
SELECT TOP 10
  person_id,
  condition_start_date,
  CAST(condition_start_date AS VARCHAR(50)) as date_as_string,
  CAST(condition_start_date AS FLOAT) as date_as_float,
  YEAR(condition_start_date) as date_year,
  MONTH(condition_start_date) as date_month,
  DAY(condition_start_date) as date_day
FROM dbo.condition_occurrence
WHERE condition_start_date IS NOT NULL
  AND condition_concept_id IN (
    4013024, 4012635, 4244087, 4091559, 4134689,
    4058705, 4112914, 4105755
  )
ORDER BY condition_start_date DESC
"

result <- querySql(connection, test_sql)
names(result) <- tolower(names(result))

cat("Raw query results:\n")
print(result)

cat("\n\nData types:\n")
str(result)

cat("\n\nChecking condition_start_date column:\n")
cat("Class:", class(result$condition_start_date), "\n")
cat("Type:", typeof(result$condition_start_date), "\n")
if (is.numeric(result$condition_start_date)) {
  cat("Numeric values:", head(result$condition_start_date), "\n")
  cat("Min:", min(result$condition_start_date, na.rm=TRUE), "\n")
  cat("Max:", max(result$condition_start_date, na.rm=TRUE), "\n")
}

disconnect(connection)