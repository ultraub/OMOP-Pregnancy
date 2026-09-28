source("tests/helpers.R"); ok <- TRUE
# Spark view builder emits typed values
captured <- NULL
db_execute.fake_conn <- function(connection, sql, ...) { captured <<- c(captured, sql); invisible(1L) }
conn <- structure(list(), class = "fake_conn"); attr(conn, "dbms") <- "spark"
cdf <- data.frame(concept_id = c(4014295L, 3009306L), concept_name = c("Single live birth", "O'Brien\\test"), category = c("LB", NA), gest_value = c(NA_real_, 3.75), min_month = c(NA_real_, NA_real_), stringsAsFactors = FALSE)
v <- suppressMessages(create_concept_temp_table(conn, cdf, "#hip_concepts")); sql <- captured[length(captured)]
ok <- check("view name without # and typed casts", v == "hip_concepts" && grepl("CAST(concept_id AS BIGINT)", sql, fixed = TRUE) && grepl("CAST(min_month AS DOUBLE)", sql, fixed = TRUE)) && ok
ok <- check("numeric values unquoted, strings escaped", grepl("(4014295,'Single live birth','LB',NULL,NULL)", sql, fixed = TRUE) && grepl("'O''Brien\\\\test'", sql, fixed = TRUE)) && ok
# Write SQL and statement splitting
w <- spark_write_table_sql("my.results.pregnancy_episodes", "stage1", c("person_id", "episode_start_date"), "episode_start_date", TRUE)
ok <- check("write SQL casts date columns", grepl("CAST(episode_start_date AS DATE) AS episode_start_date", w, fixed = TRUE)) && ok
ok <- check("statement splitter", identical(split_sql_statements("DROP VIEW IF EXISTS a;\nDROP VIEW IF EXISTS b;"), c("DROP VIEW IF EXISTS a", "DROP VIEW IF EXISTS b"))) && ok
# ESD concept extraction SQL through a fake backend (no SqlRender needed here: render/translate stubbed)
db_query.fake_conn <- function(connection, sql, ...) {
  captured <<- c(captured, sql)
  if (grepl(".concept", sql, fixed = TRUE) && grepl("LIKE", sql)) return(data.frame(CONCEPT_ID = 4059741L, CONCEPT_NAME = "Gestation period, 30 weeks"))
  data.frame(PERSON_ID = 1L, CONCEPT_ID = 4059741L, EVENT_DATE = d("2020-09-01"), DOMAIN_NAME = "Condition", VALUE_AS_NUMBER = NA_real_, VALUE_AS_STRING = NA_character_)
}
render_stub <- function(sql, ...) { a <- list(...); for (n in names(a)) sql <- gsub(paste0("@", n, "\\b"), paste(a[[n]], collapse = ","), sql); sql }
txt <- readLines("R/01_extraction/extract_cohort.R", warn = FALSE); txt <- gsub("SqlRender::render", "render_stub", txt); txt <- gsub("SqlRender::translate\\(([^,]+),[^)]*\\)", "\\1", txt)
eval(parse(text = txt))
create_concept_temp_table <- function(connection, concepts, table_name) "esd_concepts"
r <- extract_esd_timing_records(conn, "cdm", "vocab", "spark", concepts$pps_concepts, person_temp_table = "person_cohort")
ok <- check("ESD extraction: 4-table union with names joined", nrow(r$records) == 1 && r$records$concept_name == "Gestation period, 30 weeks" && length(gregexpr("UNION ALL", captured[length(captured)])[[1]]) == 3) && ok
if (!ok) quit(status = 1)
