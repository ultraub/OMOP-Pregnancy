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
#'   extraSettings = "httpPath=/sql/1.0/warehouses/warehouse_id"
#' )
#' }
create_omop_connection <- function(
  dbms = NULL,
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
  extraSettings = NULL,
  use_env = TRUE
) {
  
  # If use_env is TRUE, load from environment variables (SQL_ prefix for compatibility)
  if (use_env) {
    # Try SQL_ prefix first (for backward compatibility), then DB_ prefix
    if (is.null(dbms)) {
      dbms <- Sys.getenv("SQL_DBMS")
      if (dbms == "") dbms <- Sys.getenv("DB_TYPE")
      if (dbms == "") dbms <- Sys.getenv("OMOP_ENV")
      if (dbms == "") dbms <- "sql server"  # Default
    }
    
    if (is.null(server)) {
      server <- Sys.getenv("SQL_SERVER")
      if (server == "") server <- Sys.getenv("DB_SERVER")
    }
    
    if (is.null(database)) {
      database <- Sys.getenv("SQL_DATABASE")
      if (database == "") database <- Sys.getenv("DB_DATABASE")
    }
    
    if (is.null(port)) {
      port_str <- Sys.getenv("SQL_PORT")
      if (port_str == "") port_str <- Sys.getenv("DB_PORT")
      if (port_str != "") port <- as.numeric(port_str)
    }
    
    if (is.null(user) && !use_windows_auth) {
      user <- Sys.getenv("SQL_USER")
      if (user == "") user <- Sys.getenv("DB_USER")
    }
    
    if (is.null(password) && !use_windows_auth) {
      password <- Sys.getenv("SQL_PASSWORD")
      if (password == "") password <- Sys.getenv("DB_PASSWORD")
    }
    
    if (is.null(pathToDriver)) {
      pathToDriver <- Sys.getenv("SQL_JDBC_PATH")
      if (pathToDriver == "") pathToDriver <- Sys.getenv("JDBC_DRIVER_PATH")
      if (pathToDriver == "") pathToDriver <- "jdbc_drivers"
    }
    
    if (is.null(cdm_schema)) {
      cdm_schema <- Sys.getenv("SQL_CDM_SCHEMA")
      if (cdm_schema == "") cdm_schema <- Sys.getenv("CDM_SCHEMA")
      if (cdm_schema == "") cdm_schema <- "dbo"
    }
    
    if (is.null(vocabulary_schema)) {
      vocabulary_schema <- Sys.getenv("SQL_VOCABULARY_SCHEMA")
      if (vocabulary_schema == "") vocabulary_schema <- Sys.getenv("VOCABULARY_SCHEMA")
    }
    
    if (is.null(results_schema)) {
      results_schema <- Sys.getenv("SQL_RESULTS_SCHEMA")
      if (results_schema == "") results_schema <- Sys.getenv("RESULTS_SCHEMA")
    }
    
    # Get extra settings for Databricks
    if (is.null(extraSettings)) {
      extraSettings <- Sys.getenv("DB_EXTRA_SETTINGS")
      if (extraSettings == "") extraSettings <- NULL
    }
    
    # Check for Windows auth flag in environment
    env_windows_auth <- Sys.getenv("USE_WINDOWS_AUTH", "false")
    if (tolower(env_windows_auth) %in% c("true", "1", "yes")) {
      use_windows_auth <- TRUE
      # For Windows auth, also check if credentials were provided
      # Some environments require explicit domain credentials even with Windows auth
      if (user == "" || is.null(user)) {
        # Check for Windows-specific user variable
        win_user <- Sys.getenv("SQL_WINDOWS_USER")
        if (win_user != "") user <- win_user
      }
      if (password == "" || is.null(password)) {
        # Check for Windows-specific password variable
        win_password <- Sys.getenv("SQL_WINDOWS_PASSWORD")
        if (win_password != "") password <- win_password
      }
    }
    
    # Handle separate database configuration (previous version compatibility)
    cdm_database <- Sys.getenv("SQL_CDM_DATABASE")
    results_database <- Sys.getenv("SQL_RESULTS_DATABASE")
    
    if (cdm_database != "" && cdm_schema != "" && !grepl("\\.", cdm_schema)) {
      cdm_schema <- paste0(cdm_database, ".", cdm_schema)
    }
    if (cdm_database != "" && vocabulary_schema != "" && !grepl("\\.", vocabulary_schema)) {
      vocabulary_schema <- paste0(cdm_database, ".", vocabulary_schema)
    }
    if (results_database != "" && results_schema != "" && !grepl("\\.", results_schema)) {
      results_schema <- paste0(results_database, ".", results_schema)
    }
  }
  
  # Validate required parameters
  if (is.null(server) || server == "") {
    stop("Server is required. Set SQL_SERVER in .env or provide server parameter.")
  }
  
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
    # Windows authentication approach depends on environment
    # Check if we have Windows credentials provided
    if (!is.null(user) && user != "" && !is.null(password) && password != "") {
      # Use NTLM with explicit credentials (for cross-platform or domain auth)
      jdbc_url <- paste0(jdbc_url, "integratedSecurity=false;authenticationScheme=NTLM;")
      message("  Using Windows AD with NTLM authentication")
      
      return(DatabaseConnector::createConnectionDetails(
        dbms = "sql server",
        connectionString = jdbc_url,
        user = user,
        password = password,
        pathToDriver = pathToDriver
      ))
    } else {
      # Try true integrated security (works on Windows with proper driver setup)
      # Don't specify authenticationScheme to let driver auto-detect
      jdbc_url <- paste0(jdbc_url, "integratedSecurity=true;")
      message("  Using Windows integrated security")
      
      return(DatabaseConnector::createConnectionDetails(
        dbms = "sql server",
        connectionString = jdbc_url,
        pathToDriver = pathToDriver
      ))
    }
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
  
  # Ensure rJava is initialized for Databricks
  # This is important for proper Arrow memory management
  if (!exists(".jinit")) {
    if (require("rJava", quietly = TRUE)) {
      tryCatch({
        .jinit()
        message("  Initialized rJava for Databricks connection")
      }, error = function(e) {
        # rJava already initialized, continue
      })
    }
  }
  
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
  jdbc_url <- paste0(jdbc_url, "UseNativeQuery=0;")  # Disable native query optimization
  
  # Batch optimization note:
  # The Databricks JDBC driver does not support batch-related parameters in the connection string.
  # Performance optimization is achieved through:
  # 1. DatabaseConnector's bulk upload functionality (DATABASE_CONNECTOR_BULK_UPLOAD=TRUE)
  # 2. Application-level batch processing (DATABASE_CONNECTOR_BATCH_SIZE environment variable)
  # 3. Arrow optimization when properly configured
  # The batch_size variable is still used by our code for splitting large datasets
  
  # Only enable Arrow if explicitly requested (to avoid memory initialization errors)
  enable_arrow <- Sys.getenv("ENABLE_ARROW", "FALSE")
  if (toupper(enable_arrow) %in% c("TRUE", "1", "YES")) {
    jdbc_url <- paste0(jdbc_url, "EnableArrow=1;")
    message("  Arrow optimization enabled (requires proper JVM configuration)")
  } else {
    jdbc_url <- paste0(jdbc_url, "EnableArrow=0;")
  }
  
  # Log optimization settings if in interactive mode
  if (interactive()) {
    batch_size <- Sys.getenv("DATABASE_CONNECTOR_BATCH_SIZE", "10000")
    bulk_upload <- Sys.getenv("DATABASE_CONNECTOR_BULK_UPLOAD", "FALSE")
    message(sprintf("  Batch processing size: %s rows", batch_size))
    if (toupper(bulk_upload) %in% c("TRUE", "1", "YES")) {
      message("  Bulk upload: enabled (DatabaseConnector)")
    }
  }
  
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
  
  # Arrow optimization is now controlled at connection creation time
  # via the ENABLE_ARROW environment variable in the JDBC URL
  # This ensures consistency between JDBC and R settings
  
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
  
  # Create connection using the main function with use_env=TRUE
  # This will automatically pick up SQL_ or DB_ prefixed variables
  create_omop_connection(use_env = TRUE)
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