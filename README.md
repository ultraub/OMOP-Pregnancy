# OMOP Pregnancy

Identifies pregnancy episodes in an OMOP CDM database.

This is an R implementation of the HIPPS algorithm (Jones et al. 2023),
following the All of Us R implementation by Smith et al. (2024) and adapted
to run against a standard OMOP CDM on SQL Server, PostgreSQL, or
Databricks. The algorithm logic is kept as close to the reference as
possible; the places where this implementation deliberately differs are
listed below.

## What the pipeline does

1. **Load concept sets.** Reads the HIP and PPS concept lists and the Matcho
   term and spacing tables from `inst/extdata/`. These are the same tables
   used by the reference implementation.
2. **Extract records.** Runs one set of queries against the CDM and brings
   everything needed into R:
   - persons not recorded as male (`gender_concept_id` not in a configurable
     male list, default 8507), with year of birth within the age bounds;
   - every record in `condition_occurrence`, `procedure_occurrence`,
     `observation`, and `measurement` whose concept is in the HIP list;
   - every record in those four tables plus `visit_occurrence` whose concept
     is in the PPS list;
   - every record whose concept name contains "gestation period" or is in
     the ESD concept lists, with its concept name and value columns, for
     start-date estimation.
   Records are then restricted to those where the person was between the
   minimum and maximum age on the record date.
3. **HIP.** Builds outcome-based episodes from live birth, stillbirth,
   ectopic, abortion, and delivery codes in that order of priority, applying
   the Matcho minimum spacing between consecutive outcomes. Groups
   gestational-age records into gestation episodes, matches them to outcome
   episodes by temporal overlap, and creates PREG episodes for gestation
   data with no outcome. Start dates come from the matched gestation record,
   or from the category's maximum term when there is none. Outcomes whose
   gestation data contradicts them are reclassified to PREG rather than
   dropped.
4. **PPS.** Builds episodes from gestational-timing concepts by checking
   that consecutive records are consistent with the expected months of
   gestation, then looks ahead from each episode for an outcome code. An
   episode is described by its first and last concept dates and, if found,
   its outcome.
5. **Merge.** Joins HIP and PPS episodes on temporal overlap, resolves
   episodes that overlap more than one from the other algorithm, and
   reconciles outcome and end date when the two disagree. Nothing is
   dropped at this stage.
6. **Estimated start date (ESD) and quality flags.** Combines week-level
   and range-level timing evidence within each episode to infer the start
   date and a precision category. Episodes with no evidence get a start
   based on the category's maximum term. Adds the term-duration flag,
   outcome concordance score, and preterm flag.

Step 2 is the only database access. Everything else runs in R on data
frames.

## Requirements

- R 4.0 or later
- Java 8 or later and a JDBC driver for the database
  (`inst/scripts/setup_jdbc_drivers.R` downloads drivers)
- R packages: DatabaseConnector, SqlRender, dplyr, lubridate, readr, DBI
  (inside Databricks, sparklyr replaces DatabaseConnector; see below)

## Configuration

Connection settings are read from a `.env` file in the project root.

```bash
cp inst/templates/.env.template .env
```

The template covers SQL Server (SQL or Windows authentication), PostgreSQL,
and Databricks. Platform-specific templates with more commentary are in the
same folder. See `CONNECTION_SETUP.md` for details and troubleshooting.

The variables that matter for the analysis are `DB_TYPE`, the connection
fields, `CDM_SCHEMA`, `VOCABULARY_SCHEMA` (defaults to the CDM schema),
`RESULTS_SCHEMA` (optional), and `OUTPUT_FOLDER`.

## Running

From the project root:

```r
source("inst/scripts/run_pregnancy_analysis.R")
```

This connects using `.env`, runs every step, writes CSV and RDS files to the
output folder, and writes a `pregnancy_episodes` table to the results schema
when one is configured.

The same pipeline is available as a single function:

```r
conn <- create_connection_from_env()
episodes <- run_pregnancy_identification(
  connection = conn,
  cdm_database_schema = "cdm",
  vocabulary_database_schema = "vocab",   # optional, defaults to the CDM schema
  results_database_schema = "results",    # optional
  output_folder = "output",               # optional
  min_age = 15,
  max_age = 56
)
DatabaseConnector::disconnect(conn)
```

Each step is also exported on its own (`extract_pregnancy_cohort`,
`run_hip_algorithm`, `run_pps_algorithm`, `merge_pregnancy_episodes`,
`calculate_estimated_start_dates`, `add_episode_quality_metadata`) for
running the pipeline in pieces.

### Inside Databricks

The pipeline can run directly on a Databricks cluster's Spark session, with
no JDBC driver or token. Database access goes through a small backend
(`R/00_connection/db_backend.R`) that dispatches on the connection object,
so the same extraction code runs over DatabaseConnector or over sparklyr.

Check the repository out as a Databricks Repo, install `sparklyr`, `DBI`,
`SqlRender`, `dplyr`, `lubridate` and `readr` on the cluster, and in an R
notebook:

```r
Sys.setenv(
  OMOP_REPO_PATH    = "/Workspace/Repos/<user>/OMOP-Pregnancy",
  CDM_SCHEMA        = "omop.data",
  VOCABULARY_SCHEMA = "omop.vocabulary",
  RESULTS_SCHEMA    = "my_project.results"    # optional
)
source(file.path(Sys.getenv("OMOP_REPO_PATH"), "inst/scripts/run_in_databricks.R"))
```

Output files are written to `output/` inside the repository folder (which is
git-ignored); set `OUTPUT_FOLDER` to write elsewhere.

Or build the connection yourself with `create_spark_connection()` and call
`run_pregnancy_identification()`. Temporary views live in the notebook's
session and the results table is written with `CREATE TABLE AS SELECT`.

SqlRender needs rJava. On some Databricks Runtimes rJava does not build
until R's Java configuration is refreshed and `libtirpc-dev` is present;
`init_rjava.sh` at the repository root does both and can be attached to the
cluster as an init script.

A step-by-step walkthrough, from package installation to saved output with
the counts at each stage, is in `Databricks/run_pipeline.qmd`. The same
cells in Databricks notebook format are in
`Databricks/run_pipeline_notebook.r`, which the Repo opens directly as a
notebook.

## Output

One row per pregnancy episode.

| Column | Meaning |
|---|---|
| `person_id`, `episode_number` | Person and the episode's sequence number for that person |
| `episode_start_date`, `episode_end_date` | Inferred start and end of the pregnancy; same as the two `inferred_` columns |
| `inferred_episode_start`, `inferred_episode_end` | Start from the ESD (or end minus max term when no timing evidence); end reconciled from HIP and PPS |
| `gestational_age_days`, `gestational_age_days_calculated` | Inferred end minus inferred start |
| `recorded_episode_start`, `recorded_episode_end`, `recorded_episode_length` | Earliest and latest observed evidence, and their span in months |
| `outcome_category` | LB, SB, DELIV, ECT, AB, SA, or PREG (no outcome found) |
| `HIP_outcome_category`, `HIP_end_date`, `PPS_outcome_category`, `PPS_end_date` | What each algorithm found on its own |
| `HIP_flag`, `PPS_flag`, `algorithm_used` | Which algorithm(s) identified the episode |
| `outcome_match` | 1 if HIP and PPS agree on the outcome within 14 days |
| `precision_days`, `precision_category` | ESD precision, from `week` to `non-specific`; `week_poor-support` when only one non-overlapping week estimate exists |
| `GW_flag`, `GR3m_flag` | Whether week-level and range-level timing evidence was found |
| `intervalsCount`, `majorityOverlapCount` | ESD diagnostics: whether a range intersection existed and whether the week estimates mostly fell inside it |
| `term_duration_flag` | 1 if the inferred length is within the category's term range (PREG: at most 301 days) |
| `outcome_concordance_score` | 2 = outcomes match, term ok, week evidence; 1 = term ok and week evidence; 0 otherwise |
| `preterm_status_from_calculation` | 1 if the inferred length is under 259 days |

Term ranges, retry periods, and the outcome hierarchy are in
`inst/extdata/matcho_limits.csv`; minimum spacing between consecutive
outcomes is in `matcho_outcome_limits.csv`.

## Differences from the reference implementation

Adaptations for a standard OMOP CDM:

- Eligibility uses `gender_concept_id` with an exclude-list of male concepts,
  rather than the All of Us `sex_at_birth_concept_id`.
- Extraction uses SqlRender and temporary tables through DatabaseConnector
  instead of dbplyr against BigQuery.
- ESD timing concepts are found by querying the vocabulary's `concept`
  table; week values are read from `value_as_number` first and then from a
  numeric parse of `value_as_string`, since a generic CDM may hold them in
  either.

Deliberate departures from the reference R code, each noted in a comment at
the relevant place:

- The gestational-age measurement concepts contribute a week value only when
  it is strictly between 0 and 44, as in the N3C original. The reference R
  code also accepts any concept named "gestational age", which makes that
  bound inoperative.
- When several PPS episodes are equally close to a HIP episode, the longest
  is kept, and rows still duplicated after the resolution rounds are kept.
  This follows the N3C original and the reference's own comments; the
  reference R code assigns a different column at that point and drops the
  leftovers.
- The term table includes a PREG row (30 to 301 days), so a PREG episode
  with no timing evidence gets a start of end minus 301 days instead of NA.

## Repository layout

```
R/
  00_concepts/      concept and limits loading
  00_connection/    connection from .env or explicit settings; Spark backend
  01_extraction/    database queries and type enforcement
  02_algorithms/    HIP, PPS, merge, ESD and quality metadata
  03_results/       CSV, RDS and database output
  03_utilities/     temp table helpers
  main.R            run_pregnancy_identification()
inst/
  extdata/          concept lists and Matcho tables
  scripts/          run script, Databricks entry script, connection setup and diagnostics
  templates/        .env templates
  sql/              a standalone person query
Databricks/
  run_pipeline.qmd        step-by-step run on a Databricks cluster
  run_pipeline_notebook.r the same cells as a Databricks notebook
  qmd_to_notebook.r       converts a .qmd into Databricks notebook format
  build_evaluation_notebook.R  regenerates the evaluation notebook
Evaluation/
  validation_report.qmd   evaluation against the obstetric registry (see below)
  validation_report_notebook.r  generated notebook form for Databricks
  Pregnancy Validation Concise.py  shorter Databricks-only notebook with additional
                          diagnostics (outcome-stratified matching, unmatched
                          registry episodes, OMOP footprint of unmatched losses)
  validation_report.html  rendered report with results tables
  figures/                figures from the evaluation
ConceptSets/        ATLAS concept-set exports used by the downstream analysis
EDA_pregnancy_updated_16Sept25_for_JHU.Rmd downstream epidemiological analysis (All of Us)
```

The EDA document and the `ConceptSets` folder are not part of the pipeline.

## Evaluation

`Evaluation/validation_report.qmd` compares the pipeline output with a
registry of clinically recorded pregnancies. Two ground-truth sources are
supported through the `gt_source` parameter: the PMAP OB registry tables on
the SQL Server instance (`pmap_sqlserver`, outcomes derived from cumulative
obstetric counts) and the EDW pregnancy and birth fact tables on Databricks
(`edw_databricks`, outcomes taken from `PregnancyOutcome` and the birth-level
fetal-demise status, linked to `person_id` through `registry_idmap`). Both
produce the same episode frame, so everything downstream is shared. The
report links registry patients to `person_id`, matches predicted episodes
to registry episodes within a date window (primary analysis excludes registry pregnancies that predate the
patient's observable record; sensitivity analyses vary the window and
include everything), reports outcome classification agreement, compares
gestational age with agreement and Bland-Altman plots, aggregates pregnancy
counts per person, and compares detection of preeclampsia,
pregnancy-induced hypertension, and gestational diabetes between the two
sources.

The registry and OMOP database names, the prediction file, the matching
window, and the prediction filters are Quarto parameters at the top of the
document. Render it from the `Evaluation` folder with a `.env` in the
project root:

```bash
cd Evaluation
quarto render validation_report.qmd -P prediction_file:../output/pregnancy_episodes_YYYY-MM-DD.csv
# against the Databricks EDW tables, with the CDM at catalog omop, schema data:
quarto render validation_report.qmd -P gt_source:edw_databricks -P omop_database:omop -P omop_schema:data
```

`validation_report.html` is the most recent render and `figures/` holds
its figures.

To run the evaluation inside Databricks, open
`Evaluation/validation_report_notebook.r` from the Repo; it is the same
document in notebook form, generated by `Databricks/build_evaluation_notebook.R` (which uses `Databricks/qmd_to_notebook.r`)
with `connection_type: spark` and `gt_source: edw_databricks`, and it picks
up the newest `pregnancy_episodes_*.csv` in `output/`. Edit the `.qmd` and
rerun the build script rather than editing the notebook. Tables display as
plain data frames there instead of formatted HTML.

## Testing

There is no automated test suite yet. `inst/scripts/test_connection.R`
checks the database connection and `inst/scripts/test_full_pipeline.R` runs
the pipeline end to end against the configured database. The evaluation
report above is the accuracy check.

## References

- Jones SE, Bradwell KR, Chan LE, et al. Who Is Pregnant? Defining
  Real-World Data-Based Pregnancy Episodes in the National COVID Cohort
  Collaborative (N3C). JAMIA Open. 2023;6(3):ooad067.
  Code: https://github.com/jonessarae/n3c_pregnancy_cohort
- Smith LH, Wang W, Keefe-Oates B. Pregnancy episodes in All of Us:
  harnessing multi-source data for pregnancy-related research. JAMIA.
  2024;31(12):2789-2799.
  Code: https://github.com/louisahsmith/allofus-pregnancy
- Matcho A, Ryan P, Fife D, Gifkins D, Knoll C, Friedman A. Inferring
  pregnancy episodes and outcomes within a network of observational
  databases. PLoS One. 2018;13(2):e0192033.

## License

Apache License 2.0
