#' Load Environment Variables from .Renv file
#'
#' This function loads environment variables from a .Renv file for secure
#' credential management. It looks for .Renv in the package root directory.
#'
#' @param env_file Path to the .Renv file (default: ".Renv")
#' @param verbose Logical, whether to print loaded variables (default: FALSE)
#'
#' @return Invisibly returns a list of loaded environment variables
#' @export
#'
#' @examples
#' \dontrun{
#' # Load environment variables
#' load_env()
#' 
#' # Access variables
#' db_server <- Sys.getenv("DB_SERVER")
#' db_user <- Sys.getenv("DB_USER")
#' }
load_env <- function(env_file = ".Renv", verbose = FALSE) {
  # Check if file exists
  if (!file.exists(env_file)) {
    # Try to find it in parent directories (for when called from subdirectories)
    if (file.exists(here::here(env_file))) {
      env_file <- here::here(env_file)
    } else if (file.exists(file.path("..", env_file))) {
      env_file <- file.path("..", env_file)
    } else {
      warning(paste("Environment file not found:", env_file, 
                   "\nPlease create .Renv from .Renv.example template"))
      return(invisible(list()))
    }
  }
  
  # Read the file
  lines <- readLines(env_file, warn = FALSE)
  
  # Parse environment variables
  env_vars <- list()
  for (line in lines) {
    # Skip comments and empty lines
    if (grepl("^\\s*#", line) || grepl("^\\s*$", line)) {
      next
    }
    
    # Parse KEY=VALUE pairs
    if (grepl("^[A-Z_][A-Z0-9_]*=", line)) {
      parts <- strsplit(line, "=", fixed = TRUE)[[1]]
      if (length(parts) >= 2) {
        key <- trimws(parts[1])
        value <- trimws(paste(parts[-1], collapse = "="))
        
        # Remove quotes if present
        value <- gsub("^['\"]|['\"]$", "", value)
        
        # Set environment variable
        Sys.setenv(.new = setNames(list(value), key))
        env_vars[[key]] <- value
        
        if (verbose && !grepl("PASSWORD|SECRET|KEY", key, ignore.case = TRUE)) {
          message(paste("Loaded:", key, "=", value))
        } else if (verbose) {
          message(paste("Loaded:", key, "= [HIDDEN]"))
        }
      }
    }
  }
  
  if (verbose) {
    message(paste("Loaded", length(env_vars), "environment variables from", env_file))
  }
  
  invisible(env_vars)
}

#' Get Database Connection Details from Environment
#'
#' This function creates DatabaseConnector connection details using
#' environment variables loaded from .Renv file.
#'
#' @param env_prefix Prefix for environment variables (default: "", 
#'                   use "PG" for PostgreSQL, "ORACLE" for Oracle, etc.)
#'
#' @return DatabaseConnector connectionDetails object
#' @export
#'
#' @examples
#' \dontrun{
#' # Load environment and get connection details
#' load_env()
#' connectionDetails <- get_connection_from_env()
#' 
#' # For PostgreSQL
#' connectionDetails_pg <- get_connection_from_env("PG")
#' }
get_connection_from_env <- function(env_prefix = "") {
  # Load environment if not already loaded
  if (Sys.getenv("DB_SERVER") == "" && env_prefix == "") {
    load_env()
  }
  
  # Add underscore to prefix if provided
  if (env_prefix != "") {
    env_prefix <- paste0(env_prefix, "_")
  }
  
  # Get environment variables
  server <- Sys.getenv(paste0("DB_SERVER", env_prefix))
  port <- as.numeric(Sys.getenv(paste0("DB_PORT", env_prefix), "1433"))
  database <- Sys.getenv(paste0("DB_NAME", env_prefix))
  user <- Sys.getenv(paste0("DB_USER", env_prefix))
  password <- Sys.getenv(paste0("DB_PASSWORD", env_prefix))
  dbms <- Sys.getenv(paste0("DB_DBMS", env_prefix), "sql server")
  
  # Validate required variables
  if (server == "" || user == "" || password == "") {
    stop("Required database environment variables not found. Please check your .Renv file.")
  }
  
  # Create connection details
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = dbms,
    server = server,
    user = user,
    password = password,
    port = port,
    extraSettings = if (dbms == "sql server") paste0("database=", database) else NULL
  )
  
  return(connectionDetails)
}