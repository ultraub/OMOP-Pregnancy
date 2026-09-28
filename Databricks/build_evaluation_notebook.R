# Regenerate Evaluation/validation_report_notebook.r from the .qmd
# Run from the repository root:  Rscript Databricks/build_evaluation_notebook.R
source("Databricks/qmd_to_notebook.r")

repo_path <- "/Workspace/Users/rbarre16@jh.edu/OMOP-Pregnancy"

qmd_to_notebook(
  qmd_path = "Evaluation/validation_report.qmd",
  out_path = "Evaluation/validation_report_notebook.r",
  overrides = list(
    connection_type = "spark",
    max_start_date  = "2026-01-01",
    min_ga_days     = 0,
    gt_source       = "edw_databricks",
    prediction_file = "latest",
    edw_catalog     = "obstetrics_irb00501137",
    edw_phi_schema  = "phi",
    edw_idmap_schema = "omop",
    omop_database   = "obstetrics_irb00501137",
    omop_schema     = "omop",
    env_file        = ""
  ),
  preamble = c(
    "# Packages the report needs (installed once per cluster)",
    'needed <- c("sparklyr", "DBI", "dplyr", "tidyr", "purrr", "lubridate", "ggplot2",',
    '            "scales", "knitr", "kableExtra", "janitor", "tibble")',
    "missing <- needed[!vapply(needed, requireNamespace, logical(1), quietly = TRUE)]",
    "if (length(missing) > 0) install.packages(missing)",
    "",
    "# The report's paths are relative to the Evaluation folder of the checked-out repo",
    sprintf('repo_path <- "%s"', repo_path),
    'setwd(file.path(repo_path, "Evaluation"))',
    "",
    "# In a notebook only the last value of a cell is displayed, so the",
    "# report's tables are printed as data frames at the point they are built",
    "kable <- function(x, ...) { print(as.data.frame(x), row.names = FALSE); invisible(x) }",
    "kable_styling <- function(x, ...) invisible(x)"
  )
)
cat("Wrote Evaluation/validation_report_notebook.r\n")
