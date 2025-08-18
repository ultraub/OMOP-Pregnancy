#' Enforce Data Types
#'
#' Standardizes data types across all database platforms to ensure
#' consistent processing in R. This eliminates platform-specific
#' type issues (e.g., SQL Server returning dates as strings).
#'
#' @param data Data frame from database
#' @param domain Domain type to determine expected columns
#'
#' @return Data frame with enforced types
#' @export
enforce_types <- function(data, domain) {
  
  # Return empty data frame if input is empty
  if (is.null(data) || nrow(data) == 0) {
    return(data)
  }
  
  # Convert column names to lowercase for consistency
  names(data) <- tolower(names(data))
  
  # Apply domain-specific type enforcement
  result <- switch(
    domain,
    
    "person" = data %>%
      mutate(
        person_id = as.integer(person_id),
        gender_concept_id = as.integer(gender_concept_id),
        year_of_birth = as.integer(year_of_birth),
        month_of_birth = as.integer(coalesce(month_of_birth, 1L)),
        day_of_birth = as.integer(coalesce(day_of_birth, 1L)),
        race_concept_id = as.integer(coalesce(race_concept_id, 0L)),
        ethnicity_concept_id = as.integer(coalesce(ethnicity_concept_id, 0L))
      ),
    
    "condition" = data %>%
      mutate(
        person_id = as.integer(person_id),
        concept_id = as.integer(concept_id),
        event_date = parse_date_safe(event_date),
        concept_name = as.character(concept_name),
        category = as.character(category),
        gest_value = as.numeric(gest_value),
        value_as_number = as.numeric(value_as_number),
        value_as_string = as.character(value_as_string)
      ),
    
    "procedure" = data %>%
      mutate(
        person_id = as.integer(person_id),
        concept_id = as.integer(concept_id),
        event_date = parse_date_safe(event_date),
        concept_name = as.character(concept_name),
        category = as.character(category),
        gest_value = as.numeric(gest_value),
        value_as_number = as.numeric(value_as_number),
        value_as_string = as.character(value_as_string)
      ),
    
    "observation" = data %>%
      mutate(
        person_id = as.integer(person_id),
        concept_id = as.integer(concept_id),
        event_date = parse_date_safe(event_date),
        concept_name = as.character(concept_name),
        category = as.character(category),
        gest_value = as.numeric(gest_value),
        value_as_number = as.numeric(value_as_number),
        value_as_string = as.character(value_as_string)
      ),
    
    "measurement" = data %>%
      mutate(
        person_id = as.integer(person_id),
        concept_id = as.integer(concept_id),
        event_date = parse_date_safe(event_date),
        concept_name = as.character(concept_name),
        category = as.character(category),
        gest_value = as.numeric(gest_value),
        value_as_number = as.numeric(value_as_number),
        value_as_string = as.character(value_as_string)
      ),
    
    "gestational" = data %>%
      mutate(
        person_id = as.integer(person_id),
        concept_id = as.integer(concept_id),
        event_date = parse_date_safe(event_date),
        # Only add domain_name if it exists
        across(any_of("domain_name"), as.character),
        value_as_number = as.numeric(value_as_number),
        value_as_string = as.character(value_as_string),
        min_month = if("min_month" %in% names(.)) as.integer(min_month) else NA_integer_,
        max_month = if("max_month" %in% names(.)) as.integer(max_month) else NA_integer_
      ),
    
    # Default: just ensure basic types
    data %>%
      mutate(
        across(where(is.character) & matches("_id$"), as.integer),
        across(where(is.character) & matches("_date$"), parse_date_safe),
        across(where(is.logical), as.integer)
      )
  )
  
  return(result)
}

#' Safely parse dates from various formats
#'
#' Handles dates that might come as strings from SQL Server
#' or other formats from different databases.
#'
#' @param x Vector of dates (might be character, Date, POSIXct, etc.)
#' @return Date vector
#' @noRd
parse_date_safe <- function(x) {
  
  # If already Date class, return as-is
  if (inherits(x, "Date")) {
    return(x)
  }
  
  # If POSIXct/POSIXlt, convert to Date
  if (inherits(x, c("POSIXct", "POSIXlt"))) {
    return(as.Date(x))
  }
  
  # If character, try to parse
  if (is.character(x)) {
    # Try common formats
    result <- tryCatch({
      # Try ISO format first (most common from databases)
      lubridate::ymd(x)
    }, error = function(e) {
      # Try other formats
      tryCatch({
        parsed <- lubridate::parse_date_time(x, orders = c("ymd", "mdy", "dmy"))
        as.Date(parsed)
      }, error = function(e2) {
        # Last resort: base R
        as.Date(x)
      })
    })
    
    # Ensure we return a Date object, not POSIXct
    return(as.Date(result))
  }
  
  # If numeric (might be Excel date or Unix timestamp), convert
  if (is.numeric(x)) {
    # Check if it looks like Excel date (days since 1900-01-01)
    # Excel dates are typically between 1 and ~50000
    # Unix timestamps are much larger (seconds since 1970)
    if (all(x[!is.na(x)] < 100000)) {
      # Likely Excel date - days since 1900-01-01 (with Excel's leap year bug)
      # SQL Server might return dates as days since 1900-01-01
      return(as.Date(x, origin = "1899-12-30"))
    } else {
      # Likely Unix timestamp - convert from seconds to days
      return(as.Date(x/86400, origin = "1970-01-01"))
    }
  }
  
  # Default: try to coerce
  return(as.Date(x))
}

#' Validate data frame structure
#'
#' Ensures required columns exist and have correct types
#'
#' @param data Data frame to validate
#' @param required_columns Named list of required columns and their types
#' @return TRUE if valid, stops with error if not
#' @export
validate_data_structure <- function(data, required_columns) {
  
  # Check for missing columns
  missing_cols <- setdiff(names(required_columns), names(data))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }
  
  # Check column types
  for (col_name in names(required_columns)) {
    expected_type <- required_columns[[col_name]]
    actual_type <- class(data[[col_name]])[1]
    
    if (!inherits(data[[col_name]], expected_type)) {
      warning(sprintf(
        "Column '%s' has type '%s' but expected '%s'",
        col_name, actual_type, expected_type
      ))
    }
  }
  
  return(TRUE)
}

#' Coalesce helper function
#'
#' Returns first non-NA value, with a default
#'
#' @param x Vector
#' @param default Default value if all NA
#' @return Vector with NAs replaced by default
#' @noRd
coalesce <- function(x, default) {
  ifelse(is.na(x), default, x)
}