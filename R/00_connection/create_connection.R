#' Flexible Database Connection Management
#'
#' Functions to create database connections supporting multiple platforms including
#' SQL Server (with Windows AD authentication), Databricks, PostgreSQL, and more.
#'
#' @name create_connection
#' @import DatabaseConnector
#' @import DBI
NULL

#' Create a database connection with platform-specific optimizations
#'
#' Supports multiple database platforms with appropriate configurations:
#' - SQL Server: Standard auth and Windows AD (Integrated Security)
#' - Databricks/Spark: JDBC with Arrow optimization
#' - PostgreSQL/Redshift: Standard JDBC connections
#' - BigQuery: For All of Us environment
#'
#' @param dbms Character string: "sql server", "spark", "databricks", "postgresql", etc.
#' @param server Server name/address
#' @param database Database name
#' @param port Port number (optional, uses defaults)
#' @param user Username (optional for Windows auth)
#' @param password Password (optional for Windows auth)
#' @param use_windows_auth Logical: Use Windows AD authentication (SQL Server only)
#' @param connectionString Full connection string (optional, for advanced use)
#' @param pathToDriver Path to JDBC driver folder
#' @param cdm_schema CDM schema name
#' @param vocabulary_schema Vocabulary schema (defaults to cdm_schema)
#' @param results_schema Results schema (optional)
#' @param extraSettings Additional connection settings
#'
#' @return DatabaseConnector connection object with metadata attributes
#' @export
#'
#' @examples
#' \dontrun{
#' # SQL Server with Windows AD authentication
#' con <- create_omop_connection(
#'   dbms = "sql server",
#'   server = "server.domain.com",
#'   database = "OMOP_CDM",
#'   use_windows_auth = TRUE,
#'   cdm_schema = "dbo"
#' )
#'
#' # SQL Server with standard authentication
#' con <- create_omop_connection(
#'   dbms = "sql server",
#'   server = "server.domain.com",
#'   database = "OMOP_CDM",
#'   user = "username",
#'   password = "password",
#'   cdm_schema = "dbo"
#' )
#'
#' # Databricks with token authentication
#' con <- create_omop_connection(
#'   dbms = "databricks",
#'   server = "workspace.cloud.databricks.com",
#'   database = "default",
#'   user = "token",
#'   password = Sys.getenv("DATABRICKS_TOKEN"),
#'   cdm_schema = "omop.data",
#'   extraSettings = "HTTPPath=/sql/1.0/warehouses/warehouse_id"
#' )
#' }
create_omop_connection <- function(
  dbms,
  server = NULL,
  database = NULL,
  port = NULL,
  user = NULL,
  password = NULL,
  use_windows_auth = FALSE,
  connectionString = NULL,
  pathToDriver = "jdbc_drivers",
  cdm_schema = NULL,
  vocabulary_schema = NULL,
  results_schema = NULL,
  extraSettings = NULL
) {
  
  # Normalize dbms name
  dbms <- tolower(dbms)
  if (dbms == "databricks") dbms <- "spark"
  
  # Set default ports if not provided
  if (is.null(port)) {
    port <- switch(
      dbms,
      "sql server" = 1433,
      "postgresql" = 5432,
      "spark" = 443,
      "oracle" = 1521,
      "redshift" = 5439,
      NULL
    )
  }
  
  # Handle vocabulary schema default
  if (is.null(vocabulary_schema)) {
    vocabulary_schema <- cdm_schema
  }
  
  # Create connection details based on platform
  if (dbms == "sql server") {
    connectionDetails <- create_sqlserver_connection(
      server = server,
      database = database,
      port = port,
      user = user,
      password = password,
      use_windows_auth = use_windows_auth,
      connectionString = connectionString,
      pathToDriver = pathToDriver
    )
  } else if (dbms == "spark") {
    connectionDetails <- create_databricks_connection(
      server = server,
      database = database,
      port = port,
      user = user,
      password = password,
      connectionString = connectionString,
      pathToDriver = pathToDriver,
      extraSettings = extraSettings
    )
  } else {
    # Generic connection for other platforms
    connectionDetails <- DatabaseConnector::createConnectionDetails(
      dbms = dbms,
      server = if (!is.null(server)) paste0(server, "/", database) else NULL,
      port = port,
      user = user,
      password = password,
      connectionString = connectionString,
      pathToDriver = pathToDriver
    )
  }
  
  # Create the connection
  message(sprintf("Connecting to %s database...", dbms))
  connection <- DatabaseConnector::connect(connectionDetails)
  
  # Store metadata as attributes
  attr(connection, "dbms") <- dbms
  attr(connection, "cdm_schema") <- cdm_schema
  attr(connection, "vocabulary_schema") <- vocabulary_schema
  attr(connection, "results_schema") <- results_schema
  attr(connection, "server") <- server
  attr(connection, "database") <- database
  
  # Apply platform-specific optimizations
  if (dbms == "spark") {
    configure_spark_connection(connection, results_schema)
  }
  
  message("✓ Connection successful")
  return(connection)
}

#' Create SQL Server connection with Windows AD support
#' @noRd
create_sqlserver_connection <- function(
  server,
  database,
  port,
  user,
  password,
  use_windows_auth,
  connectionString,
  pathToDriver
) {
  
  if (!is.null(connectionString)) {
    # Use provided connection string
    return(DatabaseConnector::createConnectionDetails(
      dbms = "sql server",
      connectionString = connectionString,
      pathToDriver = pathToDriver
    ))
  }
  
  # Build JDBC URL
  jdbc_url <- sprintf(
    "jdbc:sqlserver://%s:%d;database=%s;encrypt=true;trustServerCertificate=true;",
    server, port, database
  )
  
  if (use_windows_auth) {
    # Add Windows authentication parameters
    jdbc_url <- paste0(jdbc_url, "integratedSecurity=true;authenticationScheme=NTLM;")
    
    # For Windows AD, we don't need user/password
    return(DatabaseConnector::createConnectionDetails(
      dbms = "sql server",
      connectionString = jdbc_url,
      pathToDriver = pathToDriver
    ))
  } else {
    # Standard SQL Server authentication
    return(DatabaseConnector::createConnectionDetails(
      dbms = "sql server",
      connectionString = jdbc_url,
      user = user,
      password = password,
      pathToDriver = pathToDriver
    ))
  }
}

#' Create Databricks/Spark connection with Arrow optimization
#' @noRd
create_databricks_connection <- function(
  server,
  database,
  port,
  user,
  password,
  connectionString,
  pathToDriver,
  extraSettings
) {
  
  if (!is.null(connectionString)) {
    # Use provided connection string
    return(DatabaseConnector::createConnectionDetails(
      dbms = "spark",
      connectionString = connectionString,
      pathToDriver = pathToDriver
    ))
  }
  
  # Build Databricks JDBC URL
  jdbc_url <- sprintf("jdbc:databricks://%s:%d;", server, port)
  
  # Add extra settings (like HTTPPath)
  if (!is.null(extraSettings)) {
    # Clean up extra settings
    extraSettings <- gsub("^;|;$", "", extraSettings)  # Remove leading/trailing semicolons
    jdbc_url <- paste0(jdbc_url, extraSettings, ";")
  }
  
  # Add authentication
  if (!is.null(user) && !is.null(password)) {
    if (user == "token") {
      # Token authentication
      jdbc_url <- paste0(jdbc_url, 
                        "AuthMech=3;",
                        "UID=token;",
                        "PWD=", password, ";")
    } else {
      # Username/password authentication
      jdbc_url <- paste0(jdbc_url,
                        "AuthMech=3;",
                        "UID=", user, ";",
                        "PWD=", password, ";")
    }
  }
  
  # Add performance optimizations
  jdbc_url <- paste0(jdbc_url,
                    "UseNativeQuery=0;",     # Disable native query optimization
                    "EnableArrow=1;")         # Enable Arrow for performance
  
  return(DatabaseConnector::createConnectionDetails(
    dbms = "spark",
    connectionString = jdbc_url,
    pathToDriver = pathToDriver
  ))
}

#' Configure Spark/Databricks connection settings
#' @noRd
configure_spark_connection <- function(connection, results_schema) {
  # Prevent # prefix for temp tables
  options(dbplyr.compute.defaults = list(temporary = FALSE))
  options(dbplyr.temp_prefix = "temp_")
  
  # Set temp emulation schema
  if (!is.null(results_schema)) {
    options(sqlRenderTempEmulationSchema = results_schema)
  }
  
  # Disable Arrow optimization if it causes issues
  # Can be re-enabled with environment variable
  if (Sys.getenv("ENABLE_ARROW", "FALSE") == "FALSE") {
    options(sparklyr.arrow = FALSE)
    
    # Try to disable Arrow at connection level
    tryCatch({
      DBI::dbExecute(connection, "SET spark.sql.execution.arrow.enabled = false")
    }, error = function(e) {
      # Ignore if setting fails
    })
  }
  
  invisible(NULL)
}

#' Create connection from environment variables
#'
#' Creates a database connection using settings from environment variables
#' or .env file. Supports all platforms including Windows AD authentication.
#'
#' @param env_file Path to .env file (default: ".env")
#'
#' @return DatabaseConnector connection object
#' @export
create_connection_from_env <- function(env_file = ".env") {
  
  # Load environment variables if file exists
  if (file.exists(env_file)) {
    load_env_file(env_file)
    message("✓ Loaded environment variables from ", env_file)
  }
  
  # Get database configuration from environment
  dbms <- Sys.getenv("DB_TYPE", "sql server")
  server <- Sys.getenv("DB_SERVER")
  database <- Sys.getenv("DB_DATABASE")
  port <- as.numeric(Sys.getenv("DB_PORT", "0"))
  if (port == 0) port <- NULL
  
  # Authentication
  use_windows_auth <- tolower(Sys.getenv("USE_WINDOWS_AUTH", "false")) %in% c("true", "1", "yes")
  user <- Sys.getenv("DB_USER")
  password <- Sys.getenv("DB_PASSWORD")
  
  # Schemas
  cdm_schema <- Sys.getenv("CDM_SCHEMA", "dbo")
  vocabulary_schema <- Sys.getenv("VOCABULARY_SCHEMA", cdm_schema)
  results_schema <- Sys.getenv("RESULTS_SCHEMA", "")
  if (results_schema == "") results_schema <- NULL
  
  # Additional settings
  extra_settings <- Sys.getenv("DB_EXTRA_SETTINGS", "")
  if (extra_settings == "") extra_settings <- NULL
  
  # JDBC driver path
  jdbc_folder <- Sys.getenv("JDBC_DRIVER_PATH", "jdbc_drivers")
  
  # Validate required variables
  if (server == "" || database == "") {
    stop("DB_SERVER and DB_DATABASE must be set in environment or .env file")
  }
  
  if (!use_windows_auth && (user == "" || password == "")) {
    stop("DB_USER and DB_PASSWORD required when not using Windows authentication")
  }
  
  # Create connection
  create_omop_connection(
    dbms = dbms,
    server = server,
    database = database,
    port = port,
    user = if (use_windows_auth) NULL else user,
    password = if (use_windows_auth) NULL else password,
    use_windows_auth = use_windows_auth,
    pathToDriver = jdbc_folder,
    cdm_schema = cdm_schema,
    vocabulary_schema = vocabulary_schema,
    results_schema = results_schema,
    extraSettings = extra_settings
  )
}

#' Load environment variables from file
#' @noRd
load_env_file <- function(env_file) {
  if (file.exists(env_file)) {
    env_lines <- readLines(env_file, warn = FALSE)
    for (line in env_lines) {
      line <- trimws(line)
      if (nchar(line) > 0 && !startsWith(line, "#")) {
        parts <- strsplit(line, "=", fixed = TRUE)[[1]]
        if (length(parts) >= 2) {
          key <- trimws(parts[1])
          value <- trimws(paste(parts[-1], collapse = "="))
          # Remove quotes if present
          value <- gsub("^['\"]|['\"]$", "", value)
          do.call(Sys.setenv, setNames(list(value), key))
        }
      }
    }
  }
}