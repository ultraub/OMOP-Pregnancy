#' Cross-Platform SQL Function Wrappers using SqlRender
#'
#' This module provides database-agnostic SQL function wrappers that use SqlRender
#' to generate platform-specific SQL. These functions replace hardcoded SQL Server
#' syntax with portable alternatives that work across all OHDSI-supported databases.
#'
#' @name sql_functions
#' @import SqlRender
#' @import dbplyr
NULL

#' Generate cross-platform SQL for date difference calculation
#'
#' Creates platform-specific SQL for calculating the difference between two dates.
#' This replaces direct usage of SQL Server's DATEDIFF function.
#'
#' @param date1 First date expression (later date for positive result)
#' @param date2 Second date expression (earlier date for positive result)
#' @param unit Unit of measurement ("day", "month", "year")
#' @param connection Optional database connection for dialect detection
#'
#' @return A dbplyr SQL expression for date difference
#' @export
#'
#' @examples
#' \dontrun{
#' # In a dplyr pipeline:
#' data %>%
#'   mutate(age_days = sql_date_diff("visit_date", "birth_date", "day"))
#' }
sql_date_diff <- function(date1, date2, unit = "day", connection = NULL) {
  
  # Get database dialect from connection if provided
  dbms <- NULL
  if (!is.null(connection)) {
    dbms <- attr(connection, "dbms", exact = TRUE)
  }
  
  # Default to SQL Server syntax (OHDSI standard)
  if (is.null(dbms)) {
    dbms <- "sql server"
  }
  
  # Convert unit to SQL Server format for consistency
  sql_unit <- switch(tolower(unit),
                    "day" = "day",
                    "days" = "day",
                    "month" = "month",
                    "months" = "month",
                    "year" = "year",
                    "years" = "year",
                    "day")  # default
  
  # Special handling for Spark/Databricks
  if (dbms == "spark" || dbms == "databricks") {
    # Spark uses different DATEDIFF syntax: DATEDIFF(date1, date2) returns days
    if (sql_unit == "month") {
      return(dbplyr::sql(sprintf("CAST(MONTHS_BETWEEN(%s, %s) AS INT)", date1, date2)))
    } else if (sql_unit == "year") {
      return(dbplyr::sql(sprintf("(YEAR(%s) - YEAR(%s))", date1, date2)))
    } else {
      # Default to days - Spark's DATEDIFF returns days by default
      return(dbplyr::sql(sprintf("DATEDIFF(%s, %s)", date1, date2)))
    }
  }
  
  # Create the SQL Server version (source for SqlRender)
  sql_server_sql <- sprintf("DATEDIFF(%s, %s, %s)", sql_unit, date2, date1)
  
  # Translate to target dialect
  translated_sql <- SqlRender::translate(sql_server_sql, targetDialect = dbms)
  
  # Return as dbplyr SQL expression
  return(dbplyr::sql(translated_sql))
}

#' Generate cross-platform SQL for creating a date from parts
#'
#' Creates platform-specific SQL for constructing a date from year, month, and day.
#' This replaces direct usage of SQL Server's DATEFROMPARTS function.
#'
#' @param year Year expression or column name
#' @param month Month expression or column name  
#' @param day Day expression or column name
#' @param connection Optional database connection for dialect detection
#'
#' @return A dbplyr SQL expression for date construction
#' @export
#'
#' @examples
#' \dontrun{
#' # In a dplyr pipeline:
#' data %>%
#'   mutate(birth_date = sql_date_from_parts("year_of_birth", "month_of_birth", "day_of_birth"))
#' }
sql_date_from_parts <- function(year, month, day, connection = NULL) {
  
  # Get database dialect from connection if provided
  dbms <- NULL
  if (!is.null(connection)) {
    dbms <- attr(connection, "dbms", exact = TRUE)
  }
  
  # Default to SQL Server syntax
  if (is.null(dbms)) {
    dbms <- "sql server"
  }
  
  # Create the SQL based on dialect
  if (dbms %in% c("sql server", "pdw")) {
    # SQL Server has native DATEFROMPARTS
    result_sql <- sprintf("DATEFROMPARTS(%s, %s, %s)", year, month, day)
    
  } else if (dbms == "postgresql" || dbms == "redshift") {
    # PostgreSQL/Redshift: Use string concatenation and casting
    result_sql <- sprintf("TO_DATE(CONCAT(%s::text, '-', LPAD(%s::text, 2, '0'), '-', LPAD(%s::text, 2, '0')), 'YYYY-MM-DD')",
                         year, month, day)
    
  } else if (dbms == "oracle") {
    # Oracle: Use TO_DATE with concatenation
    result_sql <- sprintf("TO_DATE(%s || '-' || LPAD(%s, 2, '0') || '-' || LPAD(%s, 2, '0'), 'YYYY-MM-DD')",
                         year, month, day)
    
  } else if (dbms == "bigquery") {
    # BigQuery: Use DATE function
    result_sql <- sprintf("DATE(%s, %s, %s)", year, month, day)
    
  } else if (dbms == "snowflake") {
    # Snowflake: Use DATE_FROM_PARTS
    result_sql <- sprintf("DATE_FROM_PARTS(%s, %s, %s)", year, month, day)
    
  } else if (dbms == "spark" || dbms == "databricks") {
    # Spark/Databricks: Use MAKE_DATE function (Spark 3.0+)
    result_sql <- sprintf("MAKE_DATE(%s, %s, %s)", year, month, day)
    
  } else {
    # Generic fallback using string concatenation
    result_sql <- sprintf("CAST(CONCAT(%s, '-', LPAD(CAST(%s AS VARCHAR), 2, '0'), '-', LPAD(CAST(%s AS VARCHAR), 2, '0')) AS DATE)",
                         year, month, day)
  }
  
  # Return as dbplyr SQL expression
  return(dbplyr::sql(result_sql))
}

#' Generate cross-platform SQL for date arithmetic
#'
#' Creates platform-specific SQL for adding or subtracting days from a date.
#' This replaces direct usage of SQL Server's DATEADD function.
#'
#' @param date Date expression or column name
#' @param days Number of days to add (negative for subtraction)
#' @param unit Unit of addition ("day", "month", "year")
#' @param connection Optional database connection for dialect detection
#'
#' @return A dbplyr SQL expression for date arithmetic
#' @export
#'
#' @examples
#' \dontrun{
#' # In a dplyr pipeline:
#' data %>%
#'   mutate(start_date = sql_date_add("end_date", -280, "day"))
#' }
sql_date_add <- function(date, days, unit = "day", connection = NULL) {
  
  # Get database dialect from connection if provided
  dbms <- NULL
  if (!is.null(connection)) {
    dbms <- attr(connection, "dbms", exact = TRUE)
  }
  
  # Default to SQL Server syntax
  if (is.null(dbms)) {
    dbms <- "sql server"
  }
  
  # Convert unit to SQL Server format
  sql_unit <- switch(tolower(unit),
                    "day" = "day",
                    "days" = "day",
                    "month" = "month",
                    "months" = "month",
                    "year" = "year",
                    "years" = "year",
                    "day")  # default
  
  # Special handling for Spark/Databricks
  if (dbms == "spark" || dbms == "databricks") {
    # Spark has DATE_ADD for days and ADD_MONTHS for months
    if (sql_unit == "month") {
      return(dbplyr::sql(sprintf("ADD_MONTHS(%s, %s)", date, days)))
    } else if (sql_unit == "year") {
      return(dbplyr::sql(sprintf("ADD_MONTHS(%s, %s * 12)", date, days)))
    } else {
      # Default to days
      return(dbplyr::sql(sprintf("DATE_ADD(%s, %s)", date, days)))
    }
  }
  
  # Create the SQL Server version (source for SqlRender)
  sql_server_sql <- sprintf("DATEADD(%s, %s, %s)", sql_unit, days, date)
  
  # Translate to target dialect
  translated_sql <- SqlRender::translate(sql_server_sql, targetDialect = dbms)
  
  # Return as dbplyr SQL expression
  return(dbplyr::sql(translated_sql))
}

#' Generate cross-platform SQL for string concatenation
#'
#' Creates platform-specific SQL for concatenating strings.
#' This replaces direct usage of CONCAT function which varies across databases.
#'
#' @param ... String expressions or column names to concatenate
#' @param connection Optional database connection for dialect detection
#'
#' @return A dbplyr SQL expression for string concatenation
#' @export
#'
#' @examples
#' \dontrun{
#' # In a dplyr pipeline:
#' data %>%
#'   mutate(id = sql_concat("person_id", "visit_date"))
#' }
sql_concat <- function(..., connection = NULL) {
  
  # Get all arguments to concatenate
  args <- list(...)
  
  # Get database dialect from connection if provided
  dbms <- NULL
  if (!is.null(connection)) {
    dbms <- attr(connection, "dbms", exact = TRUE)
  }
  
  # Default to SQL Server syntax
  if (is.null(dbms)) {
    dbms <- "sql server"
  }
  
  # Create concatenation SQL based on dialect
  if (dbms %in% c("sql server", "pdw")) {
    # SQL Server: Use CONCAT function
    result_sql <- sprintf("CONCAT(%s)", paste(args, collapse = ", "))
    
  } else if (dbms == "postgresql" || dbms == "redshift") {
    # PostgreSQL/Redshift: Use || operator
    result_sql <- paste(args, collapse = " || ")
    
  } else if (dbms == "oracle") {
    # Oracle: Use || operator
    result_sql <- paste(args, collapse = " || ")
    
  } else if (dbms == "bigquery") {
    # BigQuery: Use CONCAT function
    result_sql <- sprintf("CONCAT(%s)", paste(args, collapse = ", "))
    
  } else if (dbms == "snowflake") {
    # Snowflake: Use CONCAT function or ||
    result_sql <- sprintf("CONCAT(%s)", paste(args, collapse = ", "))
    
  } else if (dbms == "spark" || dbms == "databricks") {
    # Spark/Databricks: Use CONCAT function
    result_sql <- sprintf("CONCAT(%s)", paste(args, collapse = ", "))
    
  } else {
    # Generic: Try CONCAT function
    result_sql <- sprintf("CONCAT(%s)", paste(args, collapse = ", "))
  }
  
  # Return as dbplyr SQL expression
  return(dbplyr::sql(result_sql))
}

#' Get appropriate SQL expression from connection context
#'
#' Helper function to extract database dialect from various connection contexts
#' in dplyr pipelines or direct connections.
#'
#' @param obj A database connection, lazy tbl, or dplyr pipeline object
#'
#' @return Database dialect string or NULL
#' @export
get_sql_dialect <- function(obj) {
  
  # If it's a connection object
  if (inherits(obj, "DBIConnection")) {
    return(attr(obj, "dbms", exact = TRUE))
  }
  
  # If it's a tbl_lazy or tbl_sql (dplyr lazy table)
  if (inherits(obj, c("tbl_lazy", "tbl_sql"))) {
    if (!is.null(obj$src$con)) {
      return(attr(obj$src$con, "dbms", exact = TRUE))
    }
  }
  
  # Try to detect from class names (fallback)
  obj_class <- class(obj)[1]
  
  if (grepl("postgres", obj_class, ignore.case = TRUE)) {
    return("postgresql")
  } else if (grepl("sqlserver|mssql", obj_class, ignore.case = TRUE)) {
    return("sql server")
  } else if (grepl("oracle", obj_class, ignore.case = TRUE)) {
    return("oracle")
  } else if (grepl("bigquery", obj_class, ignore.case = TRUE)) {
    return("bigquery")
  } else if (grepl("redshift", obj_class, ignore.case = TRUE)) {
    return("redshift")
  } else if (grepl("snowflake", obj_class, ignore.case = TRUE)) {
    return("snowflake")
  } else if (grepl("spark|databricks", obj_class, ignore.case = TRUE)) {
    return("spark")
  }
  
  # Default to SQL Server (OHDSI standard)
  return("sql server")
}

#' Get DBMS from connection with fallback
#'
#' Internal helper to reliably get DBMS type from connection
#'
#' @param connection Database connection object
#'
#' @return DBMS string
#' @keywords internal
get_dbms_from_connection <- function(connection) {
  dbms <- NULL
  
  if (!is.null(connection)) {
    dbms <- attr(connection, "dbms", exact = TRUE)
  }
  
  # Default to SQL Server if not specified
  if (is.null(dbms)) {
    dbms <- "sql server"
  }
  
  return(dbms)
}

#' Translate SQL string using SqlRender for cross-platform compatibility
#'
#' This function takes SQL Server syntax and translates it to the target
#' database dialect using SqlRender. Use this for any SQL that would 
#' otherwise be passed directly to sql().
#'
#' @param sql_string SQL string in SQL Server syntax
#' @param connection Database connection for dialect detection
#'
#' @return A dbplyr SQL expression translated for the target database
#' @export
#'
#' @examples
#' \dontrun{
#' # Instead of:
#' mutate(days = sql("DATEDIFF(day, date1, date2)"))
#' 
#' # Use:
#' mutate(days = sql_translate("DATEDIFF(day, date1, date2)", connection))
#' }
sql_translate <- function(sql_string, connection = NULL) {
  
  # Get database dialect
  dbms <- get_dbms_from_connection(connection)
  
  # Special handling for Spark/Databricks - they need different syntax
  if (dbms == "spark" || dbms == "databricks") {
    # Handle common SQL Server patterns that Spark doesn't support
    
    # DATEDIFF(unit, date1, date2) -> DATEDIFF(date2, date1) for days
    if (grepl("DATEDIFF\\s*\\(\\s*day", sql_string, ignore.case = TRUE)) {
      # Extract the dates and reorder for Spark
      sql_string <- gsub("DATEDIFF\\s*\\(\\s*day\\s*,\\s*([^,]+)\\s*,\\s*([^)]+)\\s*\\)",
                        "DATEDIFF(\\2, \\1)", sql_string, ignore.case = TRUE)
    }
    
    # DATEADD(day, n, date) -> DATE_ADD(date, n)
    if (grepl("DATEADD\\s*\\(\\s*day", sql_string, ignore.case = TRUE)) {
      sql_string <- gsub("DATEADD\\s*\\(\\s*day\\s*,\\s*([^,]+)\\s*,\\s*([^)]+)\\s*\\)",
                        "DATE_ADD(\\2, \\1)", sql_string, ignore.case = TRUE)
    }
    
    # CONCAT with CAST - Spark handles this differently
    if (grepl("CONCAT\\s*\\(.*CAST", sql_string, ignore.case = TRUE)) {
      # Keep as is - Spark supports CONCAT and CAST
    }
    
    # CAST(NULL AS DATE) - ensure proper NULL handling
    sql_string <- gsub("CAST\\s*\\(\\s*NULL\\s+AS\\s+DATE\\s*\\)",
                      "CAST(NULL AS DATE)", sql_string, ignore.case = TRUE)
  } else {
    # For non-Spark databases, use SqlRender translation
    translated_sql <- SqlRender::translate(sql_string, targetDialect = dbms)
    return(dbplyr::sql(translated_sql))
  }
  
  # Return as dbplyr SQL expression
  return(dbplyr::sql(sql_string))
}

#' Create SQL expressions with automatic dialect detection
#'
#' Wrapper function that automatically detects the database dialect from
#' the pipeline context and generates appropriate SQL.
#'
#' @param .data The data object (used for dialect detection)
#' @param type Type of SQL function ("date_diff", "date_from_parts", "date_add", "concat")
#' @param ... Arguments to pass to the specific SQL function
#'
#' @return A dbplyr SQL expression
#' @export
sql_auto <- function(.data, type, ...) {
  
  # Get connection from data object
  connection <- NULL
  if (inherits(.data, c("tbl_lazy", "tbl_sql"))) {
    connection <- .data$src$con
  }
  
  # Call appropriate function based on type
  switch(type,
         "date_diff" = sql_date_diff(..., connection = connection),
         "date_from_parts" = sql_date_from_parts(..., connection = connection),
         "date_add" = sql_date_add(..., connection = connection),
         "concat" = sql_concat(..., connection = connection),
         stop("Unknown SQL function type: ", type))
}
