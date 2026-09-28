# Shared helpers for the offline regression tests (no database needed).
# Run everything with:  Rscript tests/run_all.R   (from the repository root)
suppressPackageStartupMessages({library(dplyr); library(lubridate); library(readr)})
for (dir in c("R/00_connection", "R/00_concepts", "R/01_extraction", "R/02_algorithms", "R/03_results", "R/03_utilities")) {
  for (f in list.files(dir, pattern = "\\.R$", full.names = TRUE)) source(f)
}
source("R/main.R")
concepts <- suppressMessages(load_concept_sets())
d <- function(x) as.Date(x)
rec <- function(pid, cid, date, name, cat, gv = NA_real_)
  data.frame(person_id = pid, concept_id = cid, event_date = d(date), concept_name = name, category = cat,
             gest_value = gv, value_as_number = NA_real_, value_as_string = NA_character_)
gt <- function(pid, cid, date, mn, mx)
  data.frame(person_id = pid, concept_id = cid, event_date = d(date), domain_name = "Measurement",
             value_as_number = NA_real_, value_as_string = NA_character_, min_month = mn, max_month = mx)
cohort_of <- function(conds, timing = NULL, persons = NULL) {
  pids <- unique(c(conds$person_id, timing$person_id))
  if (is.null(persons)) persons <- data.frame(person_id = pids, year_of_birth = 1990L, month_of_birth = 1L, day_of_birth = 1L)
  list(persons = persons, conditions = conds, procedures = conds[0, ], observations = conds[0, ], measurements = conds[0, ],
       gestational_timing = if (is.null(timing)) data.frame() else timing)
}
check <- function(name, expr) { ok <- tryCatch(isTRUE(all(expr)), error = function(e) { message("  error: ", conditionMessage(e)); FALSE })
  cat(sprintf("  [%s] %s\n", if (ok) "PASS" else "FAIL", name)); ok }
