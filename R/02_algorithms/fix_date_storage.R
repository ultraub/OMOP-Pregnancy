#' Safe date arithmetic function
#'
#' Ensures dates remain as Date objects after arithmetic operations
#' @noRd
safe_date_subtract <- function(date, days) {
  # Ensure input is a Date
  if (!inherits(date, "Date")) {
    date <- as.Date(date)
  }
  
  # Perform subtraction and ensure result is Date
  result <- date - days
  
  # Force back to Date class if needed
  if (!inherits(result, "Date")) {
    result <- as.Date(result)
  }
  
  return(result)
}

#' Safe date addition function
#'
#' Ensures dates remain as Date objects after arithmetic operations
#' @noRd
safe_date_add <- function(date, days) {
  # Ensure input is a Date
  if (!inherits(date, "Date")) {
    date <- as.Date(date)
  }
  
  # Perform addition and ensure result is Date
  result <- date + days
  
  # Force back to Date class if needed
  if (!inherits(result, "Date")) {
    result <- as.Date(result)
  }
  
  return(result)
}