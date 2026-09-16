#' Database backend dispatch
#'
#' The pipeline talks to the database through four operations: run a query,
#' execute a statement, write a table, and check/close the connection. These
#' generics dispatch on the connection object so the same extraction code
#' runs over a DatabaseConnector JDBC connection (SQL Server, PostgreSQL,
#' Databricks SQL warehouse) or over a Spark session inside Databricks
#' (class "omop_spark_connection", see create_spark_connection()).
#'
#' Connection attributes read elsewhere in the pipeline ("dbms",
#' "cdm_schema", "vocabulary_schema", "results_schema") are set on both
#' kinds of connection.
#' @name db_backend
NULL

#' Run a query and return a data frame
#' @param connection Connection object
#' @param sql SQL text (already translated to the target dialect)
#' @param ... Passed to the backend
#' @export
db_query <- function(connection, sql, ...) {
  UseMethod("db_query")
}

#' @export
db_query.default <- function(connection, sql, ...) {
  DatabaseConnector::querySql(connection, sql, ...)
}

#' @export
db_query.omop_spark_connection <- function(connection, sql, ...) {
  result <- DBI::dbGetQuery(connection$sc, sql)
  as.data.frame(result, stringsAsFactors = FALSE)
}

#' Execute one or more SQL statements
#' @inheritParams db_query
#' @export
db_execute <- function(connection, sql, ...) {
  UseMethod("db_execute")
}

#' @export
db_execute.default <- function(connection, sql, ...) {
  DatabaseConnector::executeSql(connection, sql, ...)
}

#' @export
db_execute.omop_spark_connection <- function(connection, sql, ...) {
  statements <- split_sql_statements(sql)
  for (statement in statements) {
    DBI::dbExecute(connection$sc, statement)
  }
  invisible(length(statements))
}

#' Split a SQL script into statements (Spark executes one at a time)
#' @noRd
split_sql_statements <- function(sql) {
  statements <- if (requireNamespace("SqlRender", quietly = TRUE)) {
    SqlRender::splitSql(sql)
  } else {
    strsplit(sql, ";[[:space:]]*(\n|$)")[[1]]
  }
  statements <- trimws(statements)
  statements[nchar(statements) > 0]
}

#' Write a data frame to a database table
#'
#' @param connection Connection object
#' @param database_schema Schema (or Databricks catalog.schema) for the table
#' @param table_name Table name
#' @param data Data frame
#' @param overwrite Replace the table if it exists (default TRUE)
#' @param ... Passed to the backend
#' @export
db_insert_table <- function(connection, database_schema, table_name, data, overwrite = TRUE, ...) {
  UseMethod("db_insert_table")
}

#' @export
db_insert_table.default <- function(connection, database_schema, table_name, data, overwrite = TRUE, ...) {
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = database_schema,
    tableName = table_name,
    data = as.data.frame(data),
    dropTableIfExists = overwrite,
    createTable = TRUE,
    tempTable = FALSE,
    progressBar = TRUE,
    ...
  )
  invisible(TRUE)
}

#' @export
db_insert_table.omop_spark_connection <- function(connection, database_schema, table_name, data, overwrite = TRUE, ...) {
  df <- as.data.frame(data, stringsAsFactors = FALSE)

  # Ship Date columns as ISO text and cast them back in the CREATE TABLE so
  # the result does not depend on how the sparklyr version serializes dates
  date_cols <- names(df)[vapply(df, function(x) inherits(x, "Date"), logical(1))]
  for (col in date_cols) df[[col]] <- format(df[[col]], "%Y-%m-%d")

  staging <- paste0("omop_pregnancy_stage_", format(Sys.time(), "%Y%m%d%H%M%S"))
  sparklyr::sdf_copy_to(connection$sc, df, name = staging, overwrite = TRUE, memory = FALSE)

  target <- if (is.null(database_schema) || database_schema == "") {
    table_name
  } else {
    paste0(database_schema, ".", table_name)
  }
  DBI::dbExecute(connection$sc, spark_write_table_sql(target, staging, names(df), date_cols, overwrite))
  DBI::dbExecute(connection$sc, paste0("DROP VIEW IF EXISTS ", staging))
  invisible(TRUE)
}

#' Build the CREATE TABLE / INSERT statement used by the Spark backend
#' @noRd
spark_write_table_sql <- function(target, staging, columns, date_cols, overwrite) {
  select_list <- vapply(columns, function(col) {
    if (col %in% date_cols) sprintf("CAST(%s AS DATE) AS %s", col, col) else col
  }, character(1))
  verb <- if (overwrite) "CREATE OR REPLACE TABLE %s AS SELECT %s FROM %s" else "INSERT INTO %s SELECT %s FROM %s"
  sprintf(verb, target, paste(select_list, collapse = ", "), staging)
}

#' Is the connection usable?
#' @param connection Connection object
#' @export
db_is_valid <- function(connection) {
  UseMethod("db_is_valid")
}

#' @export
db_is_valid.default <- function(connection) {
  isTRUE(tryCatch(DBI::dbIsValid(connection), error = function(e) TRUE))
}

#' @export
db_is_valid.omop_spark_connection <- function(connection) {
  isTRUE(tryCatch(sparklyr::connection_is_open(connection$sc), error = function(e) FALSE))
}

#' Close the connection
#' @param connection Connection object
#' @export
db_disconnect <- function(connection) {
  UseMethod("db_disconnect")
}

#' @export
db_disconnect.default <- function(connection) {
  DatabaseConnector::disconnect(connection)
}

#' @export
db_disconnect.omop_spark_connection <- function(connection) {
  if (isTRUE(connection$owns_session)) {
    sparklyr::spark_disconnect(connection$sc)
  } else {
    message("Spark session is shared with the notebook; leaving it open")
  }
  invisible(NULL)
}

#' Connection to the Spark session inside Databricks
#'
#' Wraps a sparklyr connection so the pipeline can run natively in a
#' Databricks notebook or job: no JDBC driver, no token, temporary views live
#' in the session, and results are written with CREATE TABLE AS SELECT.
#'
#' @param cdm_schema catalog.schema of the CDM tables, e.g. "omop.data"
#' @param vocabulary_schema catalog.schema of the concept table (default: cdm_schema)
#' @param results_schema catalog.schema for output tables (optional)
#' @param sc An existing sparklyr connection. If NULL, one is opened with
#'   sparklyr::spark_connect(method = method); inside Databricks this attaches
#'   to the notebook's session.
#' @param method Passed to sparklyr::spark_connect when sc is NULL
#' @return An "omop_spark_connection" object
#' @export
create_spark_connection <- function(cdm_schema,
                                    vocabulary_schema = NULL,
                                    results_schema = NULL,
                                    sc = NULL,
                                    method = "databricks") {
  if (!requireNamespace("sparklyr", quietly = TRUE)) {
    stop("Package 'sparklyr' is required for create_spark_connection()")
  }
  owns_session <- is.null(sc)
  if (owns_session) {
    sc <- sparklyr::spark_connect(method = method)
  }
  connection <- structure(
    list(sc = sc, owns_session = owns_session),
    class = "omop_spark_connection"
  )
  attr(connection, "dbms") <- "spark"
  attr(connection, "cdm_schema") <- cdm_schema
  attr(connection, "vocabulary_schema") <- if (is.null(vocabulary_schema) || vocabulary_schema == "") cdm_schema else vocabulary_schema
  attr(connection, "results_schema") <- results_schema
  connection
}

#' @export
print.omop_spark_connection <- function(x, ...) {
  cat("OMOP pregnancy Spark connection\n")
  cat("  CDM schema:        ", attr(x, "cdm_schema"), "\n")
  cat("  Vocabulary schema: ", attr(x, "vocabulary_schema"), "\n")
  cat("  Results schema:    ", if (is.null(attr(x, "results_schema"))) "(none)" else attr(x, "results_schema"), "\n")
  invisible(x)
}
