# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Pregnancy Algorithm Validation Report (Databricks)
# MAGIC
# MAGIC Concise version of the full validation, Databricks/EDW ground truth only. Covers 5 analyses: episode matching, outcome classification, GA comparison, person-level aggregation, and pregnancy complications.

# COMMAND ----------

# DBTITLE 1,setup
# MAGIC %r
# MAGIC # Parameters
# MAGIC params <- list(
# MAGIC   connection_type = "spark",
# MAGIC   prediction_file = "latest",
# MAGIC   date_window_days = 30,
# MAGIC   min_ga_days = 140,
# MAGIC   max_start_date = "2026-01-01",
# MAGIC   edw_catalog = "obstetrics_irb00501137",
# MAGIC   edw_phi_schema = "phi",
# MAGIC   edw_idmap_schema = "omop",
# MAGIC   omop_database = "obstetrics_irb00501137",
# MAGIC   omop_schema = "omop"
# MAGIC )
# MAGIC
# MAGIC # Packages
# MAGIC needed <- c("sparklyr", "DBI", "dplyr", "tidyr", "purrr", "lubridate", "ggplot2",
# MAGIC             "scales", "knitr", "kableExtra", "janitor", "tibble")
# MAGIC missing <- needed[!vapply(needed, requireNamespace, logical(1), quietly = TRUE)]
# MAGIC if (length(missing) > 0) install.packages(missing)
# MAGIC suppressPackageStartupMessages({
# MAGIC   library(dplyr); library(tidyr); library(purrr); library(lubridate)
# MAGIC   library(ggplot2); library(scales); library(knitr); library(janitor); library(tibble)
# MAGIC })
# MAGIC
# MAGIC # Working directory + connection functions
# MAGIC repo_path <- "/Workspace/Users/rbarre16@jh.edu/OMOP-Pregnancy"
# MAGIC setwd(file.path(repo_path, "Evaluation"))
# MAGIC source("../R/00_connection/create_connection.R")
# MAGIC source("../R/00_connection/db_backend.R")
# MAGIC
# MAGIC open_report_connection <- function() {
# MAGIC   create_spark_connection(cdm_schema = paste(params$omop_database, params$omop_schema, sep = "."))
# MAGIC }
# MAGIC
# MAGIC theme_set(theme_minimal(base_size = 12))
# MAGIC cat("Setup complete\n")

# COMMAND ----------

# DBTITLE 1,load-predictions
# MAGIC %r
# MAGIC # Load algorithm predictions
# MAGIC load_algorithm_predictions <- function(file_path) {
# MAGIC   stopifnot(is.character(file_path), length(file_path) == 1L, nzchar(file_path))
# MAGIC   if (!file.exists(file_path)) stop("File not found: ", file_path)
# MAGIC   ext <- tolower(tools::file_ext(file_path))
# MAGIC   predictions <- if (ext == "rds") readRDS(file_path)
# MAGIC     else if (ext %in% c("csv", "txt")) read.csv(file_path, stringsAsFactors = FALSE, check.names = FALSE)
# MAGIC     else stop("Unsupported file extension: .", ext)
# MAGIC   to_date_safe <- function(x) {
# MAGIC     if (inherits(x, "Date")) return(x)
# MAGIC     if (inherits(x, "POSIXt")) return(as.Date(x))
# MAGIC     if (is.numeric(x)) return(as.Date(x, origin = "1970-01-01"))
# MAGIC     suppressWarnings(as.Date(x))
# MAGIC   }
# MAGIC   predictions <- predictions %>% janitor::clean_names()
# MAGIC   if (!"episode_start_date" %in% names(predictions) && "episode_start_date_x" %in% names(predictions))
# MAGIC     predictions$episode_start_date <- predictions$episode_start_date_x
# MAGIC   if (!"episode_end_date" %in% names(predictions) && "episode_end_date_x" %in% names(predictions))
# MAGIC     predictions$episode_end_date <- predictions$episode_end_date_x
# MAGIC   required_cols <- c("person_id", "episode_number", "episode_start_date", "episode_end_date",
# MAGIC                      "outcome_category", "gestational_age_days", "algorithm_used", "precision_category")
# MAGIC   missing <- setdiff(required_cols, names(predictions))
# MAGIC   if (length(missing) > 0) stop("Missing required columns: ", paste(missing, collapse = ", "))
# MAGIC   predictions <- predictions %>%
# MAGIC     dplyr::mutate(
# MAGIC       person_id = suppressWarnings(as.integer(person_id)),
# MAGIC       episode_number = suppressWarnings(as.integer(episode_number)),
# MAGIC       episode_start_date = to_date_safe(episode_start_date),
# MAGIC       episode_end_date = to_date_safe(episode_end_date),
# MAGIC       gestational_age_days = suppressWarnings(as.integer(gestational_age_days)),
# MAGIC       outcome_category = as.character(outcome_category),
# MAGIC       algorithm_used = as.character(algorithm_used),
# MAGIC       precision_category = as.character(precision_category)
# MAGIC     ) %>%
# MAGIC     dplyr::select(dplyr::all_of(required_cols)) %>%
# MAGIC     dplyr::distinct() %>%
# MAGIC     dplyr::filter(episode_start_date < as.Date(params$max_start_date))
# MAGIC   return(predictions)
# MAGIC }
# MAGIC
# MAGIC prediction_file <- params$prediction_file
# MAGIC if (identical(prediction_file, "latest")) {
# MAGIC   candidates <- list.files("../output", pattern = "^pregnancy_analysis_.*\\.csv$", full.names = TRUE)
# MAGIC   if (length(candidates) == 0) stop("No pregnancy_analysis_*.csv found in ../output")
# MAGIC   prediction_file <- candidates[which.max(file.info(candidates)$mtime)]
# MAGIC }
# MAGIC cat("Prediction file:", prediction_file, "\n")
# MAGIC algo_predictions <- load_algorithm_predictions(prediction_file) %>%
# MAGIC   dplyr::filter(gestational_age_days >= params$min_ga_days)
# MAGIC cat("Loaded", nrow(algo_predictions), "algorithm predictions for",
# MAGIC     dplyr::n_distinct(algo_predictions$person_id), "unique persons\n")

# COMMAND ----------

# DBTITLE 1,edw-ground-truth
# MAGIC %r
# MAGIC # Connect + query EDW ground truth
# MAGIC con <- open_report_connection()
# MAGIC stopifnot(!is.null(con), db_is_valid(con))
# MAGIC cat("Database connection established\n")
# MAGIC
# MAGIC edw_catalog <- params$edw_catalog
# MAGIC edw_phi_schema <- params$edw_phi_schema
# MAGIC edw_idmap_schema <- params$edw_idmap_schema
# MAGIC edw_table <- function(schema, table) paste0(edw_catalog, ".", schema, ".", table)
# MAGIC
# MAGIC edw_sql <- paste0("
# MAGIC   WITH births AS (
# MAGIC     SELECT PregnancyKey,
# MAGIC            COUNT(*) AS n_births,
# MAGIC            SUM(CASE WHEN BornAlive = 1 THEN 1 ELSE 0 END) AS n_born_alive,
# MAGIC            SUM(CASE WHEN LivingStatus = 'Fetal Demise' THEN 1 ELSE 0 END) AS n_fetal_demise,
# MAGIC            MAX(GestationalAgeDays) AS ga_days_birth
# MAGIC     FROM ", edw_table(edw_phi_schema, "edw_birthfact"), "
# MAGIC     GROUP BY PregnancyKey
# MAGIC   )
# MAGIC   SELECT r.person_id, p.cohort_id, 
# MAGIC          p.PregnancyKey AS pregnancy_key,
# MAGIC          p.PregnancyOutcome AS pregnancy_outcome, 
# MAGIC          p.HasDelivery AS has_delivery,
# MAGIC          p.HadFetalDemise AS had_fetal_demise,
# MAGIC          p.PregnancyEstimatedStartDate AS pregnancy_estimated_start_date,
# MAGIC          p.EpisodeStartDate AS episode_start_date,
# MAGIC          p.PregnancyEstimatedEndDate AS pregnancy_estimated_end_date,
# MAGIC          p.EpisodeEndDate AS episode_end_date,
# MAGIC          p.LastDeliveryDate AS last_delivery_date,
# MAGIC          p.LastDeliveryGestationalAge AS last_delivery_gestational_age,
# MAGIC          b.n_births, b.n_born_alive, b.n_fetal_demise, b.ga_days_birth
# MAGIC   FROM ", edw_table(edw_phi_schema, "edw_pregnancyfact"), " p
# MAGIC   INNER JOIN ", edw_table(edw_idmap_schema, "registry_idmap"), " r ON p.cohort_id = r.PMAP_id
# MAGIC   LEFT JOIN births b ON p.PregnancyKey = b.PregnancyKey
# MAGIC   WHERE p.PregnancyOutcome IS NULL OR p.PregnancyOutcome <> '*Deleted'")
# MAGIC
# MAGIC edw_raw <- db_query(con, edw_sql) %>% clean_names()
# MAGIC cat("Loaded", nrow(edw_raw), "EDW pregnancy episodes for", n_distinct(edw_raw$person_id), "linked persons\n")
# MAGIC
# MAGIC to_date_any <- function(x) {
# MAGIC   if (inherits(x, "Date")) return(x)
# MAGIC   if (inherits(x, "POSIXt")) return(as.Date(x))
# MAGIC   suppressWarnings(as.Date(as.character(x)))
# MAGIC }
# MAGIC
# MAGIC edw_episodes <- edw_raw %>%
# MAGIC   mutate(
# MAGIC     pregnancy_outcome = as.character(pregnancy_outcome),
# MAGIC     has_delivery = suppressWarnings(as.numeric(has_delivery)),
# MAGIC     had_fetal_demise = suppressWarnings(as.numeric(had_fetal_demise)),
# MAGIC     n_births = dplyr::coalesce(suppressWarnings(as.numeric(n_births)), 0),
# MAGIC     n_born_alive = dplyr::coalesce(suppressWarnings(as.numeric(n_born_alive)), 0),
# MAGIC     n_fetal_demise = dplyr::coalesce(suppressWarnings(as.numeric(n_fetal_demise)), 0),
# MAGIC     gt_start = dplyr::coalesce(to_date_any(pregnancy_estimated_start_date), to_date_any(episode_start_date)),
# MAGIC     gt_end = case_when(
# MAGIC       !is.na(has_delivery) & has_delivery == 1 & !is.na(to_date_any(last_delivery_date)) ~ to_date_any(last_delivery_date),
# MAGIC       TRUE ~ dplyr::coalesce(to_date_any(pregnancy_estimated_end_date), to_date_any(episode_end_date))
# MAGIC     ),
# MAGIC     gt_ga_days = dplyr::coalesce(suppressWarnings(as.numeric(last_delivery_gestational_age)),
# MAGIC                                  suppressWarnings(as.numeric(ga_days_birth))),
# MAGIC     is_delivery_outcome = pregnancy_outcome %in% c("Term", "Preterm"),
# MAGIC     all_fetal_demise = (n_births > 0 & n_fetal_demise == n_births & n_born_alive == 0) |
# MAGIC       (n_births == 0 & dplyr::coalesce(had_fetal_demise, 0) == 1),
# MAGIC     gt_outcome_category = case_when(
# MAGIC       is_delivery_outcome & all_fetal_demise ~ "SB",
# MAGIC       is_delivery_outcome ~ "LB",
# MAGIC       pregnancy_outcome == "Spontaneous Abortion" ~ "SA",
# MAGIC       pregnancy_outcome %in% c("Induced Abortion", "Abortion") ~ "AB",
# MAGIC       pregnancy_outcome == "Ectopic" ~ "ECT",
# MAGIC       pregnancy_outcome == "Molar" ~ "AB",
# MAGIC       TRUE ~ "PREG"
# MAGIC     ),
# MAGIC     gt_molar = pregnancy_outcome == "Molar",
# MAGIC     gt_unresolved = pregnancy_outcome %in% c("Gravida", "Para", "*Unspecified")
# MAGIC   ) %>%
# MAGIC   filter(!is.na(person_id), !is.na(gt_start) | !is.na(gt_end)) %>%
# MAGIC   filter(is.na(gt_start) | gt_start < as.Date(params$max_start_date))
# MAGIC
# MAGIC gt_episodes <- edw_episodes %>%
# MAGIC   group_by(person_id) %>%
# MAGIC   arrange(dplyr::coalesce(gt_end, gt_start), .by_group = TRUE) %>%
# MAGIC   mutate(gt_episode_num = row_number()) %>%
# MAGIC   ungroup() %>%
# MAGIC   select(person_id, cohort_id, pregnancy_key, gt_episode_num, gt_start, gt_end,
# MAGIC          gt_outcome_category, gt_ga_days, pregnancy_outcome, gt_molar, gt_unresolved)
# MAGIC
# MAGIC cat("Derived outcomes for", nrow(gt_episodes), "ground truth episodes\n")
# MAGIC cat("Person overlap (algo ∩ GT):", length(intersect(unique(algo_predictions$person_id), unique(gt_episodes$person_id))), "\n")
# MAGIC gt_episodes %>% count(pregnancy_outcome, gt_outcome_category) %>% arrange(desc(n))
# MAGIC
# MAGIC # Close connection (reopen later for complications)
# MAGIC db_disconnect(con)

# COMMAND ----------

# DBTITLE 1,episode-matching
# MAGIC %r
# MAGIC # Episode-level matching: primary (eligible GT) + sensitivity analyses
# MAGIC prepare_validation_inputs <- function(algo_episodes, gt_episodes, historical_buffer_days = 30) {
# MAGIC   algo <- algo_episodes %>%
# MAGIC     dplyr::select(person_id, episode_number, episode_start_date, episode_end_date, outcome_category, gestational_age_days) %>%
# MAGIC     dplyr::rename(algo_episode_num = episode_number, algo_outcome = outcome_category, algo_ga_days = gestational_age_days) %>%
# MAGIC     dplyr::mutate(algo_start = as.Date(episode_start_date), algo_end = as.Date(episode_end_date),
# MAGIC                   algo_start = dplyr::coalesce(algo_start, algo_end), algo_end = dplyr::coalesce(algo_end, algo_start)) %>%
# MAGIC     dplyr::filter(!is.na(person_id), !is.na(algo_start), !is.na(algo_end)) %>%
# MAGIC     dplyr::select(person_id, algo_episode_num, algo_start, algo_end, algo_outcome, algo_ga_days) %>%
# MAGIC     dplyr::group_by(person_id) %>% dplyr::arrange(algo_end, .by_group = TRUE) %>%
# MAGIC     dplyr::mutate(algo_order = dplyr::row_number()) %>% dplyr::ungroup()
# MAGIC   gt_all <- gt_episodes %>%
# MAGIC     dplyr::select(person_id, gt_episode_num, gt_start, gt_end, gt_outcome_category, gt_ga_days) %>%
# MAGIC     dplyr::rename(gt_outcome = gt_outcome_category) %>%
# MAGIC     dplyr::mutate(gt_start = as.Date(gt_start), gt_end = as.Date(gt_end),
# MAGIC                   gt_start = dplyr::coalesce(gt_start, gt_end), gt_end = dplyr::coalesce(gt_end, gt_start)) %>%
# MAGIC     dplyr::filter(!is.na(person_id), !is.na(gt_start), !is.na(gt_end)) %>%
# MAGIC     dplyr::group_by(person_id) %>% dplyr::arrange(gt_end, .by_group = TRUE) %>%
# MAGIC     dplyr::mutate(gt_order = dplyr::row_number()) %>% dplyr::ungroup()
# MAGIC   person_anchor <- algo %>% dplyr::group_by(person_id) %>%
# MAGIC     dplyr::summarise(first_algo_start = min(algo_start, na.rm = TRUE), last_algo_end = max(algo_end, na.rm = TRUE), .groups = "drop")
# MAGIC   gt_all <- gt_all %>% dplyr::left_join(person_anchor, by = "person_id") %>%
# MAGIC     dplyr::mutate(likely_historical = dplyr::if_else(!is.na(first_algo_start) & gt_end < (first_algo_start - historical_buffer_days), TRUE, FALSE, missing = FALSE),
# MAGIC                   eligible_primary = !likely_historical)
# MAGIC   gt_primary <- gt_all %>% dplyr::filter(eligible_primary)
# MAGIC   historical_summary <- tibble::tibble(total_gt_episodes = nrow(gt_all),
# MAGIC     likely_historical_gt_episodes = sum(gt_all$likely_historical, na.rm = TRUE),
# MAGIC     eligible_gt_episodes = nrow(gt_primary),
# MAGIC     pct_historical = ifelse(nrow(gt_all) > 0, 100 * sum(gt_all$likely_historical, na.rm = TRUE) / nrow(gt_all), NA_real_))
# MAGIC   list(algo = algo, gt_all = gt_all, gt_primary = gt_primary, historical_summary = historical_summary)
# MAGIC }
# MAGIC
# MAGIC build_match_candidates <- function(algo, gt) {
# MAGIC   common_persons <- intersect(unique(algo$person_id), unique(gt$person_id))
# MAGIC   cat("Persons in algorithm:", length(unique(algo$person_id)), "\n")
# MAGIC   cat("Persons in ground truth:", length(unique(gt$person_id)), "\n")
# MAGIC   cat("Person overlap for matching:", length(common_persons), "\n")
# MAGIC   if (length(common_persons) == 0) { warning("No overlap"); return(tibble::tibble()) }
# MAGIC   algo_sub <- algo %>% dplyr::filter(person_id %in% common_persons) %>% dplyr::mutate(.join_key = 1L)
# MAGIC   gt_sub <- gt %>% dplyr::filter(person_id %in% common_persons) %>% dplyr::mutate(.join_key = 1L)
# MAGIC   algo_sub %>%
# MAGIC     dplyr::inner_join(gt_sub, by = c("person_id", ".join_key"), relationship = "many-to-many") %>%
# MAGIC     dplyr::select(-.join_key) %>%
# MAGIC     dplyr::mutate(overlap_start = pmax(algo_start, gt_start), overlap_end = pmin(algo_end, gt_end),
# MAGIC                   overlap_days = pmax(0, as.numeric(overlap_end - overlap_start) + 1), overlap_any = overlap_days > 0,
# MAGIC                   start_diff_days = abs(as.numeric(algo_start - gt_start)), end_diff_days = abs(as.numeric(algo_end - gt_end)),
# MAGIC                   mid_algo = algo_start + floor(as.numeric(algo_end - algo_start) / 2),
# MAGIC                   mid_gt = gt_start + floor(as.numeric(gt_end - gt_start) / 2),
# MAGIC                   midpoint_diff_days = abs(as.numeric(mid_algo - mid_gt)),
# MAGIC                   algo_key = paste(person_id, algo_episode_num, sep = "::"),
# MAGIC                   gt_key = paste(person_id, gt_episode_num, sep = "::"))
# MAGIC }
# MAGIC
# MAGIC greedy_one_to_one_match <- function(candidates, algo, gt, rule = c("overlap", "window"), date_window_days = 30) {
# MAGIC   rule <- match.arg(rule)
# MAGIC   empty_result <- list(matched = tibble::tibble(), unmatched_algo = algo, unmatched_gt = gt,
# MAGIC     stats = list(total_algo = nrow(algo), total_gt = nrow(gt), matched = 0L, match_rate_algo = 0, match_rate_gt = 0))
# MAGIC   if (nrow(candidates) == 0) return(empty_result)
# MAGIC   cr <- candidates
# MAGIC   if (rule == "overlap") cr <- cr %>% dplyr::filter(overlap_any) %>% dplyr::arrange(person_id, dplyr::desc(overlap_days), end_diff_days, start_diff_days, midpoint_diff_days, algo_order, gt_order)
# MAGIC   if (rule == "window") cr <- cr %>% dplyr::filter(end_diff_days <= date_window_days) %>% dplyr::arrange(person_id, end_diff_days, start_diff_days, midpoint_diff_days, dplyr::desc(overlap_days), algo_order, gt_order)
# MAGIC   if (nrow(cr) == 0) return(empty_result)
# MAGIC   used_algo <- character(0); used_gt <- character(0); keep <- logical(nrow(cr))
# MAGIC   for (i in seq_len(nrow(cr))) {
# MAGIC     a <- cr$algo_key[i]; g <- cr$gt_key[i]
# MAGIC     if (!(a %in% used_algo) && !(g %in% used_gt)) { keep[i] <- TRUE; used_algo <- c(used_algo, a); used_gt <- c(used_gt, g) }
# MAGIC   }
# MAGIC   matched <- cr[keep, , drop = FALSE]
# MAGIC   list(matched = matched,
# MAGIC        unmatched_algo = algo %>% dplyr::filter(!(paste(person_id, algo_episode_num, sep = "::") %in% matched$algo_key)),
# MAGIC        unmatched_gt = gt %>% dplyr::filter(!(paste(person_id, gt_episode_num, sep = "::") %in% matched$gt_key)),
# MAGIC        stats = list(total_algo = nrow(algo), total_gt = nrow(gt), matched = nrow(matched),
# MAGIC                     match_rate_algo = ifelse(nrow(algo) > 0, nrow(matched)/nrow(algo), 0),
# MAGIC                     match_rate_gt = ifelse(nrow(gt) > 0, nrow(matched)/nrow(gt), 0)))
# MAGIC }
# MAGIC
# MAGIC run_matching_analyses <- function(algo_episodes, gt_episodes, historical_buffer_days = 30) {
# MAGIC   prepared <- prepare_validation_inputs(algo_episodes, gt_episodes, historical_buffer_days)
# MAGIC   algo <- prepared$algo; gt_all <- prepared$gt_all; gt_primary <- prepared$gt_primary
# MAGIC   cp <- build_match_candidates(algo, gt_primary)
# MAGIC   primary_overlap <- greedy_one_to_one_match(cp, algo, gt_primary, "overlap")
# MAGIC   ew14 <- greedy_one_to_one_match(cp, algo, gt_primary, "window", 14)
# MAGIC   ew30 <- greedy_one_to_one_match(cp, algo, gt_primary, "window", 30)
# MAGIC   ew60 <- greedy_one_to_one_match(cp, algo, gt_primary, "window", 60)
# MAGIC   ca <- build_match_candidates(algo, gt_all)
# MAGIC   all_gt_overlap <- greedy_one_to_one_match(ca, algo, gt_all, "overlap")
# MAGIC   summary_table <- tibble::tibble(
# MAGIC     matching_rule = c("Primary: any episode overlap (eligible GT only)", "Sensitivity: any episode overlap (all GT episodes)",
# MAGIC                       "Sensitivity: end date within \u00b114 days (eligible GT only)", "Sensitivity: end date within \u00b130 days (eligible GT only)",
# MAGIC                       "Sensitivity: end date within \u00b160 days (eligible GT only)"),
# MAGIC     matched_episodes = c(primary_overlap$stats$matched, all_gt_overlap$stats$matched, ew14$stats$matched, ew30$stats$matched, ew60$stats$matched),
# MAGIC     algorithm_match_rate = c(primary_overlap$stats$match_rate_algo, all_gt_overlap$stats$match_rate_algo, ew14$stats$match_rate_algo, ew30$stats$match_rate_algo, ew60$stats$match_rate_algo),
# MAGIC     ground_truth_match_rate = c(primary_overlap$stats$match_rate_gt, all_gt_overlap$stats$match_rate_gt, ew14$stats$match_rate_gt, ew30$stats$match_rate_gt, ew60$stats$match_rate_gt))
# MAGIC   list(algo = algo, gt_all = gt_all, gt_primary = gt_primary, historical_summary = prepared$historical_summary,
# MAGIC        candidates_primary = cp, candidates_all_gt = ca, primary_overlap = primary_overlap,
# MAGIC        all_gt_overlap = all_gt_overlap, end_window_14 = ew14, end_window_30 = ew30, end_window_60 = ew60, summary_table = summary_table)
# MAGIC }
# MAGIC
# MAGIC match_results <- run_matching_analyses(algo_predictions, gt_episodes, 30)
# MAGIC matched_episodes <- match_results$primary_overlap$matched
# MAGIC
# MAGIC cat("Historical GT Filtering Summary:\n")
# MAGIC cat("- Total GT episodes:", match_results$historical_summary$total_gt_episodes, "\n")
# MAGIC cat("- Likely historical removed:", match_results$historical_summary$likely_historical_gt_episodes, "\n")
# MAGIC cat("- Eligible GT episodes:", match_results$historical_summary$eligible_gt_episodes, "\n")
# MAGIC cat("- Percent historical:", round(match_results$historical_summary$pct_historical, 1), "%\n\n")
# MAGIC cat("Primary overlap matching:\n")
# MAGIC cat("- Algorithm episodes:", match_results$primary_overlap$stats$total_algo, "\n")
# MAGIC cat("- Eligible GT episodes:", match_results$primary_overlap$stats$total_gt, "\n")
# MAGIC cat("- Matched:", match_results$primary_overlap$stats$matched, "\n")
# MAGIC cat("- Algorithm match rate:", round(100 * match_results$primary_overlap$stats$match_rate_algo, 1), "%\n")
# MAGIC cat("- GT match rate:", round(100 * match_results$primary_overlap$stats$match_rate_gt, 1), "%\n")

# COMMAND ----------

# DBTITLE 1,matching-summary
# MAGIC %r
# MAGIC # Matching summary table
# MAGIC match_results$summary_table %>%
# MAGIC   dplyr::mutate(
# MAGIC     algorithm_match_rate = round(100 * algorithm_match_rate, 1),
# MAGIC     ground_truth_match_rate = round(100 * ground_truth_match_rate, 1)
# MAGIC   )
# MAGIC
# MAGIC cat("\nEpisode Matching Summary Across Configurations:\n\n")
# MAGIC print(knitr::kable(
# MAGIC   match_results$summary_table %>%
# MAGIC     dplyr::mutate(algorithm_match_rate = round(100 * algorithm_match_rate, 1),
# MAGIC                   ground_truth_match_rate = round(100 * ground_truth_match_rate, 1)),
# MAGIC   format = "simple",
# MAGIC   col.names = c("Matching Rule", "Matched Episodes", "Algorithm Match Rate (%)", "GT Match Rate (%)")))
# MAGIC
# MAGIC # --- Outcome-stratified matching (primary overlap, eligible GT) ---
# MAGIC gt_primary <- match_results$gt_primary
# MAGIC matched_gt_keys <- matched_episodes %>% dplyr::pull(gt_key)
# MAGIC algo_matched_keys <- matched_episodes %>% dplyr::pull(algo_key)
# MAGIC
# MAGIC # GT side: match rate per GT outcome
# MAGIC gt_by_outcome <- gt_primary %>%
# MAGIC   dplyr::mutate(gt_key = paste(person_id, gt_episode_num, sep = "::"),
# MAGIC                 matched = gt_key %in% matched_gt_keys) %>%
# MAGIC   dplyr::group_by(gt_outcome) %>%
# MAGIC   dplyr::summarise(gt_total = dplyr::n(), gt_matched = sum(matched),
# MAGIC                    gt_match_rate = round(100 * mean(matched), 1), .groups = "drop") %>%
# MAGIC   dplyr::arrange(dplyr::desc(gt_total))
# MAGIC
# MAGIC # Algo side: match rate per algo outcome
# MAGIC algo_by_outcome <- algo_predictions %>%
# MAGIC   dplyr::mutate(algo_key = paste(person_id, episode_number, sep = "::"),
# MAGIC                 matched = algo_key %in% algo_matched_keys) %>%
# MAGIC   dplyr::group_by(outcome_category) %>%
# MAGIC   dplyr::summarise(algo_total = dplyr::n(), algo_matched = sum(matched),
# MAGIC                    algo_match_rate = round(100 * mean(matched), 1), .groups = "drop") %>%
# MAGIC   dplyr::arrange(dplyr::desc(algo_total))
# MAGIC
# MAGIC cat("\nOutcome-Stratified Matching (primary overlap, eligible GT):\n\n")
# MAGIC cat("GT side (match rate = % of GT episodes matched):\n")
# MAGIC print(knitr::kable(gt_by_outcome, format = "simple",
# MAGIC   col.names = c("GT Outcome", "Total", "Matched", "Match Rate (%)")))
# MAGIC
# MAGIC cat("\nAlgorithm side (match rate = % of algo episodes matched):\n")
# MAGIC print(knitr::kable(algo_by_outcome, format = "simple",
# MAGIC   col.names = c("Algo Outcome", "Total", "Matched", "Match Rate (%)")))
# MAGIC
# MAGIC # Cross-tabulation: matched pairs by GT outcome x algo outcome
# MAGIC cross_tab <- matched_episodes %>%
# MAGIC   dplyr::count(gt_outcome, algo_outcome, .drop = FALSE) %>%
# MAGIC   tidyr::pivot_wider(names_from = algo_outcome, values_from = n, values_fill = 0) %>%
# MAGIC   dplyr::arrange(gt_outcome)
# MAGIC
# MAGIC cat("\nMatched episode cross-tabulation (rows=GT outcome, cols=Algo outcome):\n")
# MAGIC print(knitr::kable(cross_tab, format = "simple"))

# COMMAND ----------

# DBTITLE 1,unmatched-gt-deep-dive
# MAGIC %r
# MAGIC # Deep dive: why eligible GT episodes go unmatched
# MAGIC unmatched_gt <- match_results$primary_overlap$unmatched_gt
# MAGIC matched_gt <- matched_episodes %>% dplyr::select(person_id, gt_episode_num, gt_start, gt_end, gt_outcome, gt_ga_days, gt_key)
# MAGIC gt_primary <- match_results$gt_primary
# MAGIC
# MAGIC cat("Unmatched eligible GT episodes:", nrow(unmatched_gt), "of", nrow(gt_primary),
# MAGIC     "(", round(100 * nrow(unmatched_gt) / nrow(gt_primary), 1), "%)\n\n")
# MAGIC
# MAGIC # --- 1. Match rate by outcome category ---
# MAGIC outcome_breakdown <- gt_primary %>%
# MAGIC   dplyr::mutate(matched = paste(person_id, gt_episode_num, sep = "::") %in% matched_gt$gt_key) %>%
# MAGIC   dplyr::group_by(gt_outcome) %>%
# MAGIC   dplyr::summarise(total = dplyr::n(), matched_n = sum(matched), unmatched_n = sum(!matched),
# MAGIC                    match_rate = round(100 * mean(matched), 1), .groups = "drop") %>%
# MAGIC   dplyr::arrange(match_rate)
# MAGIC
# MAGIC cat("1. Match rate by GT outcome category:\n")
# MAGIC print(knitr::kable(outcome_breakdown, format = "simple",
# MAGIC   col.names = c("GT Outcome", "Total Eligible", "Matched", "Unmatched", "Match Rate (%)")))
# MAGIC
# MAGIC # --- 2. Person-level: are unmatched GT from persons the algo never saw? ---
# MAGIC algo_person_set <- unique(algo_predictions$person_id)
# MAGIC unmatched_gt_persons <- unmatched_gt %>%
# MAGIC   dplyr::mutate(algo_has_person = person_id %in% algo_person_set) %>%
# MAGIC   dplyr::group_by(algo_has_person) %>%
# MAGIC   dplyr::summarise(n_episodes = dplyr::n(), n_persons = dplyr::n_distinct(person_id), .groups = "drop")
# MAGIC
# MAGIC cat("\n2. Unmatched GT episodes — does the algorithm have ANY episode for that person?\n")
# MAGIC print(knitr::kable(unmatched_gt_persons, format = "simple",
# MAGIC   col.names = c("Algo Has Person", "Unmatched Episodes", "Unique Persons")))
# MAGIC
# MAGIC # --- 3. Among persons the algo DID see: excess GT episodes per person ---
# MAGIC person_counts <- gt_primary %>%
# MAGIC   dplyr::group_by(person_id) %>%
# MAGIC   dplyr::summarise(gt_n = dplyr::n(), .groups = "drop") %>%
# MAGIC   dplyr::left_join(
# MAGIC     algo_predictions %>% dplyr::group_by(person_id) %>% dplyr::summarise(algo_n = dplyr::n(), .groups = "drop"),
# MAGIC     by = "person_id"
# MAGIC   ) %>%
# MAGIC   dplyr::mutate(algo_n = dplyr::coalesce(algo_n, 0L), excess_gt = gt_n - algo_n)
# MAGIC
# MAGIC cat("\n3. GT vs Algo episode counts per person (eligible GT persons):\n")
# MAGIC cat("   Persons where GT > Algo:", sum(person_counts$excess_gt > 0), "\n")
# MAGIC cat("   Total excess GT episodes:", sum(pmax(person_counts$excess_gt, 0)), "\n")
# MAGIC cat("   Persons where GT == Algo:", sum(person_counts$excess_gt == 0), "\n")
# MAGIC cat("   Persons where GT < Algo:", sum(person_counts$excess_gt < 0), "\n")
# MAGIC
# MAGIC excess_by_bucket <- person_counts %>%
# MAGIC   dplyr::mutate(bucket = dplyr::case_when(
# MAGIC     excess_gt < 0 ~ "Algo > GT",
# MAGIC     excess_gt == 0 ~ "Equal",
# MAGIC     excess_gt == 1 ~ "GT excess = 1",
# MAGIC     excess_gt == 2 ~ "GT excess = 2",
# MAGIC     excess_gt <= 5 ~ "GT excess 3-5",
# MAGIC     TRUE ~ "GT excess 6+"
# MAGIC   )) %>%
# MAGIC   dplyr::group_by(bucket) %>%
# MAGIC   dplyr::summarise(n_persons = dplyr::n(), total_excess = sum(pmax(excess_gt, 0)), .groups = "drop") %>%
# MAGIC   dplyr::arrange(dplyr::desc(n_persons))
# MAGIC
# MAGIC print(knitr::kable(excess_by_bucket, format = "simple",
# MAGIC   col.names = c("Count Relationship", "Persons", "Total Excess GT Episodes")))
# MAGIC
# MAGIC # --- 4. Outcome distribution of unmatched GT, split by algo-has-person ---
# MAGIC unmatched_by_outcome_person <- unmatched_gt %>%
# MAGIC   dplyr::mutate(algo_has_person = ifelse(person_id %in% algo_person_set, "Algo has person", "Algo missing person")) %>%
# MAGIC   dplyr::group_by(algo_has_person, gt_outcome) %>%
# MAGIC   dplyr::summarise(n = dplyr::n(), .groups = "drop") %>%
# MAGIC   tidyr::pivot_wider(names_from = algo_has_person, values_from = n, values_fill = 0) %>%
# MAGIC   dplyr::arrange(dplyr::desc(`Algo has person` + `Algo missing person`))
# MAGIC
# MAGIC cat("\n4. Unmatched GT outcome breakdown (algo-has-person vs algo-missing-person):\n")
# MAGIC print(knitr::kable(unmatched_by_outcome_person, format = "simple"))
# MAGIC
# MAGIC # --- 5. Episode duration of matched vs unmatched GT ---
# MAGIC duration_comparison <- gt_primary %>%
# MAGIC   dplyr::mutate(
# MAGIC     matched = paste(person_id, gt_episode_num, sep = "::") %in% matched_gt$gt_key,
# MAGIC     duration_days = as.numeric(gt_end - gt_start)
# MAGIC   ) %>%
# MAGIC   dplyr::group_by(matched, gt_outcome) %>%
# MAGIC   dplyr::summarise(
# MAGIC     n = dplyr::n(),
# MAGIC     median_duration = round(stats::median(duration_days, na.rm = TRUE)),
# MAGIC     mean_duration = round(mean(duration_days, na.rm = TRUE)),
# MAGIC     pct_under_30d = round(100 * mean(duration_days < 30, na.rm = TRUE), 1),
# MAGIC     .groups = "drop"
# MAGIC   ) %>%
# MAGIC   dplyr::arrange(gt_outcome, matched)
# MAGIC
# MAGIC cat("\n5. Episode duration (days): matched vs unmatched, by outcome:\n")
# MAGIC print(knitr::kable(duration_comparison, format = "simple",
# MAGIC   col.names = c("Matched", "Outcome", "N", "Median Dur", "Mean Dur", "% < 30 days")))
# MAGIC
# MAGIC # --- 6. GA availability: matched vs unmatched ---
# MAGIC ga_avail <- gt_primary %>%
# MAGIC   dplyr::mutate(matched = paste(person_id, gt_episode_num, sep = "::") %in% matched_gt$gt_key) %>%
# MAGIC   dplyr::group_by(matched) %>%
# MAGIC   dplyr::summarise(n = dplyr::n(), has_ga = sum(!is.na(gt_ga_days)),
# MAGIC                    pct_has_ga = round(100 * mean(!is.na(gt_ga_days)), 1),
# MAGIC                    median_ga = round(stats::median(gt_ga_days, na.rm = TRUE)), .groups = "drop")
# MAGIC
# MAGIC cat("\n6. GA availability: matched vs unmatched GT:\n")
# MAGIC print(knitr::kable(ga_avail, format = "simple",
# MAGIC   col.names = c("Matched", "N", "Has GA", "% Has GA", "Median GA (days)")))
# MAGIC
# MAGIC # --- 7. Year distribution of unmatched ---
# MAGIC cat("\n7. Unmatched GT episodes by end-year and outcome:\n")
# MAGIC unmatched_by_year <- unmatched_gt %>%
# MAGIC   dplyr::mutate(end_year = as.integer(format(gt_end, "%Y"))) %>%
# MAGIC   dplyr::group_by(end_year, gt_outcome) %>%
# MAGIC   dplyr::summarise(n = dplyr::n(), .groups = "drop") %>%
# MAGIC   tidyr::pivot_wider(names_from = gt_outcome, values_from = n, values_fill = 0) %>%
# MAGIC   dplyr::arrange(end_year)
# MAGIC
# MAGIC print(knitr::kable(unmatched_by_year, format = "simple"))

# COMMAND ----------

# DBTITLE 1,omop-footprint-unmatched-sa-ab-ect
# MAGIC %r
# MAGIC # =========================================================
# MAGIC # OMOP clinical footprint for unmatched SA/AB/ECT episodes
# MAGIC # Question: do these GT episodes have ANY coded data in
# MAGIC # condition_occurrence or procedure_occurrence that the
# MAGIC # algorithm could have used?
# MAGIC # =========================================================
# MAGIC
# MAGIC # Reopen connection
# MAGIC con <- open_report_connection()
# MAGIC stopifnot(!is.null(con), db_is_valid(con))
# MAGIC
# MAGIC omop_db <- params$omop_database
# MAGIC omop_sch <- params$omop_schema
# MAGIC tbl <- function(t) paste0(omop_db, ".", omop_sch, ".", t)
# MAGIC
# MAGIC # Unmatched SA/AB/ECT from the deep-dive (2016+ only for modern EHR era)
# MAGIC unmatched_early <- match_results$primary_overlap$unmatched_gt %>%
# MAGIC   dplyr::filter(gt_outcome %in% c("SA", "AB", "ECT")) %>%
# MAGIC   dplyr::mutate(gt_start = as.Date(gt_start), gt_end = as.Date(gt_end)) %>%
# MAGIC   dplyr::filter(gt_end >= as.Date("2016-01-01"))
# MAGIC
# MAGIC cat("Unmatched SA/AB/ECT episodes:", nrow(unmatched_early), "\n")
# MAGIC cat("Unique persons:", dplyr::n_distinct(unmatched_early$person_id), "\n\n")
# MAGIC
# MAGIC # --- Sample random subset for efficient querying ---
# MAGIC set.seed(42)
# MAGIC sample_n_per_outcome <- 2000
# MAGIC sample_eps <- unmatched_early %>%
# MAGIC   dplyr::slice_sample(prop = 1) %>%
# MAGIC   dplyr::group_by(gt_outcome) %>%
# MAGIC   dplyr::slice_head(n = sample_n_per_outcome) %>%
# MAGIC   dplyr::ungroup()
# MAGIC cat("Sampled", nrow(sample_eps), "episodes for OMOP footprint check\n\n")
# MAGIC
# MAGIC sample_persons <- unique(sample_eps$person_id)
# MAGIC person_sql <- paste(sample_persons, collapse = ", ")
# MAGIC
# MAGIC # --- 1. condition_occurrence: ANY conditions for these persons ---
# MAGIC cat("Querying condition_occurrence...\n")
# MAGIC cond_sql <- paste0(
# MAGIC   "SELECT person_id, CAST(condition_start_date AS DATE) AS cond_date, ",
# MAGIC   "condition_concept_id ",
# MAGIC   "FROM ", tbl("condition_occurrence"), " ",
# MAGIC   "WHERE person_id IN (", person_sql, ") ",
# MAGIC   "AND condition_concept_id <> 0")
# MAGIC cond_raw <- db_query(con, cond_sql) %>% tibble::as_tibble() %>%
# MAGIC   dplyr::mutate(cond_date = as.Date(cond_date))
# MAGIC cat("Total condition records for sampled persons:", nrow(cond_raw), "\n")
# MAGIC
# MAGIC # --- 2. procedure_occurrence: ANY procedures for these persons ---
# MAGIC cat("Querying procedure_occurrence...\n")
# MAGIC proc_sql <- paste0(
# MAGIC   "SELECT person_id, CAST(procedure_date AS DATE) AS proc_date, ",
# MAGIC   "procedure_concept_id ",
# MAGIC   "FROM ", tbl("procedure_occurrence"), " ",
# MAGIC   "WHERE person_id IN (", person_sql, ") ",
# MAGIC   "AND procedure_concept_id <> 0")
# MAGIC proc_raw <- db_query(con, proc_sql) %>% tibble::as_tibble() %>%
# MAGIC   dplyr::mutate(proc_date = as.Date(proc_date))
# MAGIC cat("Total procedure records for sampled persons:", nrow(proc_raw), "\n\n")
# MAGIC
# MAGIC # --- 3. Match conditions to episode windows ---
# MAGIC # Use a 30-day buffer around gt_start/gt_end (since many have 0-day duration)
# MAGIC cond_in_window <- sample_eps %>%
# MAGIC   dplyr::select(person_id, gt_outcome, gt_start, gt_end, gt_episode_num) %>%
# MAGIC   dplyr::inner_join(cond_raw, by = "person_id", relationship = "many-to-many") %>%
# MAGIC   dplyr::filter(cond_date >= (gt_start - 30) & cond_date <= (gt_end + 30))
# MAGIC
# MAGIC proc_in_window <- sample_eps %>%
# MAGIC   dplyr::select(person_id, gt_outcome, gt_start, gt_end, gt_episode_num) %>%
# MAGIC   dplyr::inner_join(proc_raw, by = "person_id", relationship = "many-to-many") %>%
# MAGIC   dplyr::filter(proc_date >= (gt_start - 30) & proc_date <= (gt_end + 30))
# MAGIC
# MAGIC # --- 4. Per-episode: how many have ANY condition or procedure in window? ---
# MAGIC episode_keys <- sample_eps %>% dplyr::mutate(ep_key = paste(person_id, gt_episode_num, sep = "::"))
# MAGIC
# MAGIC has_cond <- cond_in_window %>%
# MAGIC   dplyr::mutate(ep_key = paste(person_id, gt_episode_num, sep = "::")) %>%
# MAGIC   dplyr::distinct(ep_key) %>% dplyr::pull(ep_key)
# MAGIC
# MAGIC has_proc <- proc_in_window %>%
# MAGIC   dplyr::mutate(ep_key = paste(person_id, gt_episode_num, sep = "::")) %>%
# MAGIC   dplyr::distinct(ep_key) %>% dplyr::pull(ep_key)
# MAGIC
# MAGIC footprint_summary <- episode_keys %>%
# MAGIC   dplyr::mutate(
# MAGIC     has_any_condition = ep_key %in% has_cond,
# MAGIC     has_any_procedure = ep_key %in% has_proc,
# MAGIC     has_any_omop = has_any_condition | has_any_procedure
# MAGIC   ) %>%
# MAGIC   dplyr::group_by(gt_outcome) %>%
# MAGIC   dplyr::summarise(
# MAGIC     n_sampled = dplyr::n(),
# MAGIC     has_condition = sum(has_any_condition),
# MAGIC     has_procedure = sum(has_any_procedure),
# MAGIC     has_any = sum(has_any_omop),
# MAGIC     pct_condition = round(100 * mean(has_any_condition), 1),
# MAGIC     pct_procedure = round(100 * mean(has_any_procedure), 1),
# MAGIC     pct_any = round(100 * mean(has_any_omop), 1),
# MAGIC     .groups = "drop"
# MAGIC   )
# MAGIC
# MAGIC cat("5. OMOP footprint within +/-30 days of GT episode window (sampled episodes):\n")
# MAGIC print(knitr::kable(footprint_summary, format = "simple",
# MAGIC   col.names = c("GT Outcome", "Sampled", "Has Cond", "Has Proc", "Has Any",
# MAGIC                 "% Cond", "% Proc", "% Any")))
# MAGIC
# MAGIC # --- 5. Among episodes WITH conditions: what are the top concepts? ---
# MAGIC cat("\n6. Top condition concepts within +/-30d window of unmatched SA/AB/ECT episodes:\n")
# MAGIC top_concepts_sql <- paste0(
# MAGIC   "SELECT ca.descendant_concept_id, c.concept_name, c.domain_id, c.vocabulary_id, c.concept_class_id ",
# MAGIC   "FROM ", tbl("concept"), " c ",
# MAGIC   "WHERE c.concept_id = ca.descendant_concept_id")
# MAGIC
# MAGIC # Get concept names for conditions found in window
# MAGIC cond_concept_ids <- unique(cond_in_window$condition_concept_id)
# MAGIC if (length(cond_concept_ids) > 0) {
# MAGIC   concept_lookup_sql <- paste0(
# MAGIC     "SELECT concept_id, concept_name, vocabulary_id, concept_class_id ",
# MAGIC     "FROM ", tbl("concept"), " ",
# MAGIC     "WHERE concept_id IN (", paste(cond_concept_ids[1:min(5000, length(cond_concept_ids))], collapse = ","), ")")
# MAGIC   concept_names <- db_query(con, concept_lookup_sql) %>% tibble::as_tibble()
# MAGIC
# MAGIC   top_conds_by_outcome <- cond_in_window %>%
# MAGIC     dplyr::count(gt_outcome, condition_concept_id, name = "n_episodes") %>%
# MAGIC     dplyr::left_join(concept_names, by = c("condition_concept_id" = "concept_id")) %>%
# MAGIC     dplyr::group_by(gt_outcome) %>%
# MAGIC     dplyr::slice_max(order_by = n_episodes, n = 10) %>%
# MAGIC     dplyr::ungroup() %>%
# MAGIC     dplyr::select(gt_outcome, condition_concept_id, concept_name, vocabulary_id, n_episodes) %>%
# MAGIC     dplyr::arrange(gt_outcome, dplyr::desc(n_episodes))
# MAGIC
# MAGIC   for (oc in c("SA", "AB", "ECT")) {
# MAGIC     cat("\n  Top conditions for unmatched", oc, ":\n")
# MAGIC     sub <- top_conds_by_outcome %>% dplyr::filter(gt_outcome == oc)
# MAGIC     if (nrow(sub) > 0) print(knitr::kable(sub %>% dplyr::select(-gt_outcome), format = "simple"))
# MAGIC     else cat("  (none)\n")
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC # --- 6. Top procedure concepts ---
# MAGIC proc_concept_ids <- unique(proc_in_window$procedure_concept_id)
# MAGIC if (length(proc_concept_ids) > 0) {
# MAGIC   proc_lookup_sql <- paste0(
# MAGIC     "SELECT concept_id, concept_name, vocabulary_id, concept_class_id ",
# MAGIC     "FROM ", tbl("concept"), " ",
# MAGIC     "WHERE concept_id IN (", paste(proc_concept_ids[1:min(5000, length(proc_concept_ids))], collapse = ","), ")")
# MAGIC   proc_names <- db_query(con, proc_lookup_sql) %>% tibble::as_tibble()
# MAGIC
# MAGIC   cat("\n7. Top procedure concepts within +/-30d window of unmatched SA/AB/ECT:\n")
# MAGIC   top_procs_by_outcome <- proc_in_window %>%
# MAGIC     dplyr::count(gt_outcome, procedure_concept_id, name = "n_episodes") %>%
# MAGIC     dplyr::left_join(proc_names, by = c("procedure_concept_id" = "concept_id")) %>%
# MAGIC     dplyr::group_by(gt_outcome) %>%
# MAGIC     dplyr::slice_max(order_by = n_episodes, n = 10) %>%
# MAGIC     dplyr::ungroup() %>%
# MAGIC     dplyr::select(gt_outcome, procedure_concept_id, concept_name, vocabulary_id, n_episodes) %>%
# MAGIC     dplyr::arrange(gt_outcome, dplyr::desc(n_episodes))
# MAGIC
# MAGIC   for (oc in c("SA", "AB", "ECT")) {
# MAGIC     cat("\n  Top procedures for unmatched", oc, ":\n")
# MAGIC     sub <- top_procs_by_outcome %>% dplyr::filter(gt_outcome == oc)
# MAGIC     if (nrow(sub) > 0) print(knitr::kable(sub %>% dplyr::select(-gt_outcome), format = "simple"))
# MAGIC     else cat("  (none)\n")
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC # --- 7. Episodes with NO OMOP footprint at all ---
# MAGIC no_footprint <- episode_keys %>%
# MAGIC   dplyr::filter(!(ep_key %in% has_cond) & !(ep_key %in% has_proc))
# MAGIC
# MAGIC cat("\n8. Episodes with ZERO OMOP conditions or procedures within +/-30d:\n")
# MAGIC no_fp_summary <- no_footprint %>%
# MAGIC   dplyr::group_by(gt_outcome) %>%
# MAGIC   dplyr::summarise(n_no_footprint = dplyr::n(), .groups = "drop") %>%
# MAGIC   dplyr::left_join(footprint_summary %>% dplyr::select(gt_outcome, n_sampled), by = "gt_outcome") %>%
# MAGIC   dplyr::mutate(pct_no_footprint = round(100 * n_no_footprint / n_sampled, 1))
# MAGIC
# MAGIC print(knitr::kable(no_fp_summary, format = "simple",
# MAGIC   col.names = c("GT Outcome", "No Footprint", "Sampled", "% No Footprint")))
# MAGIC
# MAGIC # Cleanup
# MAGIC db_disconnect(con)
# MAGIC cat("\nDone.\n")

# COMMAND ----------

# DBTITLE 1,outcome-classification
# MAGIC %r
# MAGIC # Exploratory outcome classification
# MAGIC create_confusion_matrix <- function(matched_episodes) {
# MAGIC   outcome_order <- c("LB", "SB", "ECT", "AB", "SA")
# MAGIC   matched_episodes %>%
# MAGIC     dplyr::filter(algo_outcome %in% outcome_order, gt_outcome %in% outcome_order) %>%
# MAGIC     dplyr::mutate(algo_outcome = factor(algo_outcome, levels = outcome_order),
# MAGIC                   gt_outcome = factor(gt_outcome, levels = outcome_order)) %>%
# MAGIC     dplyr::count(gt_outcome, algo_outcome, .drop = FALSE) %>%
# MAGIC     tidyr::pivot_wider(names_from = algo_outcome, values_from = n, values_fill = 0) %>%
# MAGIC     dplyr::arrange(gt_outcome)
# MAGIC }
# MAGIC
# MAGIC calculate_classification_metrics <- function(matched_episodes) {
# MAGIC   outcome_order <- c("LB", "SB", "ECT", "AB", "SA")
# MAGIC   dat <- matched_episodes %>% dplyr::filter(algo_outcome %in% outcome_order, gt_outcome %in% outcome_order)
# MAGIC   if (nrow(dat) == 0) return(list(overall_accuracy = NA_real_, total_correct = 0L, total_matched = 0L, by_category = tibble::tibble()))
# MAGIC   metrics_by_category <- purrr::map_dfr(outcome_order, function(cat) {
# MAGIC     tp <- sum(dat$algo_outcome == cat & dat$gt_outcome == cat, na.rm = TRUE)
# MAGIC     fp <- sum(dat$algo_outcome == cat & dat$gt_outcome != cat, na.rm = TRUE)
# MAGIC     fn <- sum(dat$algo_outcome != cat & dat$gt_outcome == cat, na.rm = TRUE)
# MAGIC     precision <- ifelse(tp + fp > 0, tp / (tp + fp), NA_real_)
# MAGIC     recall <- ifelse(tp + fn > 0, tp / (tp + fn), NA_real_)
# MAGIC     f1_score <- ifelse(!is.na(precision) && !is.na(recall) && (precision + recall) > 0, 2*precision*recall/(precision+recall), NA_real_)
# MAGIC     tibble::tibble(category = cat, support = sum(dat$gt_outcome == cat), predicted = sum(dat$algo_outcome == cat),
# MAGIC                    tp = tp, fp = fp, fn = fn, precision = precision, recall = recall, f1_score = f1_score)
# MAGIC   })
# MAGIC   total_correct <- sum(dat$algo_outcome == dat$gt_outcome, na.rm = TRUE)
# MAGIC   list(overall_accuracy = total_correct / nrow(dat), total_correct = total_correct, total_matched = nrow(dat), by_category = metrics_by_category)
# MAGIC }
# MAGIC
# MAGIC conf_matrix <- create_confusion_matrix(matched_episodes)
# MAGIC class_metrics <- calculate_classification_metrics(matched_episodes)
# MAGIC
# MAGIC cat("Exploratory Outcome Classification (excludes unresolved categories):\n")
# MAGIC cat("- Overall accuracy:", round(100 * class_metrics$overall_accuracy, 1), "%\n")
# MAGIC cat("(", class_metrics$total_correct, "/", class_metrics$total_matched, "episodes)\n\n")
# MAGIC
# MAGIC cat("Confusion Matrix (rows=GT, cols=Algorithm):\n")
# MAGIC print(knitr::kable(conf_matrix, format = "simple"))
# MAGIC
# MAGIC cat("\nPer-Category Metrics:\n")
# MAGIC print(knitr::kable(
# MAGIC   class_metrics$by_category %>% dplyr::mutate(dplyr::across(c(precision, recall, f1_score), ~round(., 3))),
# MAGIC   format = "simple"))

# COMMAND ----------

# DBTITLE 1,ga-comparison
# MAGIC %r
# MAGIC # Gestational age comparison
# MAGIC calculate_ga_metrics <- function(matched_episodes, min_ga = 0, max_ga = 320) {
# MAGIC   with_ga <- matched_episodes %>%
# MAGIC     dplyr::filter(!is.na(algo_ga_days), !is.na(gt_ga_days),
# MAGIC                   algo_ga_days >= min_ga, algo_ga_days <= max_ga,
# MAGIC                   gt_ga_days >= min_ga, gt_ga_days <= max_ga) %>%
# MAGIC     dplyr::mutate(ga_diff = algo_ga_days - gt_ga_days, abs_ga_diff = abs(ga_diff))
# MAGIC   if (nrow(with_ga) == 0) return(list(summary = tibble::tibble(), by_outcome = tibble::tibble(), data = with_ga))
# MAGIC   summary <- with_ga %>% dplyr::summarise(
# MAGIC     n = dplyr::n(), mean_diff_days = mean(ga_diff, na.rm = TRUE), median_diff_days = stats::median(ga_diff, na.rm = TRUE),
# MAGIC     mean_abs_diff_days = mean(abs_ga_diff, na.rm = TRUE), median_abs_diff_days = stats::median(abs_ga_diff, na.rm = TRUE),
# MAGIC     within_7_days = mean(abs_ga_diff <= 7, na.rm = TRUE), within_14_days = mean(abs_ga_diff <= 14, na.rm = TRUE),
# MAGIC     within_21_days = mean(abs_ga_diff <= 21, na.rm = TRUE))
# MAGIC   by_outcome <- with_ga %>% dplyr::group_by(gt_outcome) %>% dplyr::summarise(
# MAGIC     n = dplyr::n(), mean_diff_days = mean(ga_diff, na.rm = TRUE), median_diff_days = stats::median(ga_diff, na.rm = TRUE),
# MAGIC     mean_abs_diff_days = mean(abs_ga_diff, na.rm = TRUE),
# MAGIC     within_7_days = mean(abs_ga_diff <= 7, na.rm = TRUE), within_14_days = mean(abs_ga_diff <= 14, na.rm = TRUE),
# MAGIC     .groups = "drop") %>% dplyr::arrange(dplyr::desc(n))
# MAGIC   list(summary = summary, by_outcome = by_outcome, data = with_ga)
# MAGIC }
# MAGIC
# MAGIC ga_metrics <- calculate_ga_metrics(matched_episodes)
# MAGIC
# MAGIC cat("Gestational Age Results (primary overlap-matched):\n")
# MAGIC cat("- N:", ga_metrics$summary$n, "\n")
# MAGIC cat("- Mean GA diff:", round(ga_metrics$summary$mean_diff_days, 1), "days\n")
# MAGIC cat("- Mean abs diff:", round(ga_metrics$summary$mean_abs_diff_days, 1), "days\n")
# MAGIC cat("- Within +/-7d:", round(100 * ga_metrics$summary$within_7_days, 1), "%\n")
# MAGIC cat("- Within +/-14d:", round(100 * ga_metrics$summary$within_14_days, 1), "%\n")
# MAGIC cat("- Within +/-21d:", round(100 * ga_metrics$summary$within_21_days, 1), "%\n\n")
# MAGIC
# MAGIC cat("GA by Outcome:\n")
# MAGIC print(knitr::kable(
# MAGIC   ga_metrics$by_outcome %>%
# MAGIC     dplyr::mutate(dplyr::across(c(mean_diff_days, mean_abs_diff_days), ~round(., 1)),
# MAGIC                   dplyr::across(c(within_7_days, within_14_days), ~round(100 * ., 1))),
# MAGIC   format = "simple",
# MAGIC   col.names = c("Outcome", "N", "Mean Diff", "Median Diff", "Mean Abs Diff", "Within 7d (%)", "Within 14d (%)")))

# COMMAND ----------

# DBTITLE 1,ga-plots
# MAGIC %r
# MAGIC # GA scatter plot + Bland-Altman
# MAGIC if (nrow(ga_metrics$data) > 0) {
# MAGIC   set.seed(42)
# MAGIC   plot_data <- if (nrow(ga_metrics$data) > 50000) dplyr::slice_sample(ga_metrics$data, n = 50000) else ga_metrics$data
# MAGIC
# MAGIC   p1 <- ggplot(plot_data, aes(x = gt_ga_days, y = algo_ga_days)) +
# MAGIC     geom_point(alpha = 0.05, size = 0.5) +
# MAGIC     geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
# MAGIC     labs(title = "Algorithm vs Ground Truth GA", x = "GT GA (days)", y = "Algorithm GA (days)") +
# MAGIC     coord_fixed()
# MAGIC   print(p1)
# MAGIC
# MAGIC   # Bland-Altman
# MAGIC   ba <- plot_data %>% dplyr::mutate(mean_ga = (algo_ga_days + gt_ga_days) / 2, diff_ga = algo_ga_days - gt_ga_days)
# MAGIC   mean_diff <- mean(ba$diff_ga, na.rm = TRUE)
# MAGIC   sd_diff <- sd(ba$diff_ga, na.rm = TRUE)
# MAGIC   p2 <- ggplot(ba, aes(x = mean_ga, y = diff_ga)) +
# MAGIC     geom_point(alpha = 0.05, size = 0.5) +
# MAGIC     geom_hline(yintercept = mean_diff, color = "blue") +
# MAGIC     geom_hline(yintercept = mean_diff + 1.96 * sd_diff, color = "red", linetype = "dashed") +
# MAGIC     geom_hline(yintercept = mean_diff - 1.96 * sd_diff, color = "red", linetype = "dashed") +
# MAGIC     labs(title = "Bland-Altman Plot: GA Agreement", x = "Mean GA (days)", y = "Difference (Algo - GT, days)") +
# MAGIC     annotate("text", x = Inf, y = mean_diff, label = paste0("Mean: ", round(mean_diff, 1)), hjust = 1.1, color = "blue") +
# MAGIC     annotate("text", x = Inf, y = mean_diff + 1.96 * sd_diff, label = paste0("+1.96 SD: ", round(mean_diff + 1.96*sd_diff, 1)), hjust = 1.1, color = "red") +
# MAGIC     annotate("text", x = Inf, y = mean_diff - 1.96 * sd_diff, label = paste0("-1.96 SD: ", round(mean_diff - 1.96*sd_diff, 1)), hjust = 1.1, color = "red")
# MAGIC   print(p2)
# MAGIC }

# COMMAND ----------

# DBTITLE 1,person-level
# MAGIC %r
# MAGIC # Person-level aggregation
# MAGIC calculate_person_metrics <- function(algo_episodes, gt_episodes) {
# MAGIC   algo_counts <- algo_episodes %>% dplyr::group_by(person_id) %>% dplyr::summarise(algo_total = dplyr::n(), .groups = "drop")
# MAGIC   gt_counts <- gt_episodes %>% dplyr::group_by(person_id) %>% dplyr::summarise(gt_total = dplyr::n(), .groups = "drop")
# MAGIC   person_data <- dplyr::full_join(algo_counts, gt_counts, by = "person_id") %>%
# MAGIC     dplyr::mutate(algo_total = dplyr::coalesce(algo_total, 0L), gt_total = dplyr::coalesce(gt_total, 0L),
# MAGIC                   total_diff = algo_total - gt_total, abs_total_diff = abs(total_diff),
# MAGIC                   exact_match = algo_total == gt_total, within_1 = abs_total_diff <= 1)
# MAGIC   summary <- person_data %>% dplyr::summarise(n_persons = dplyr::n(), exact_match_rate = mean(exact_match, na.rm = TRUE),
# MAGIC     within_1_rate = mean(within_1, na.rm = TRUE), mean_abs_diff = mean(abs_total_diff, na.rm = TRUE),
# MAGIC     median_abs_diff = stats::median(abs_total_diff, na.rm = TRUE))
# MAGIC   list(summary = summary, person_data = person_data)
# MAGIC }
# MAGIC
# MAGIC person_metrics_primary <- calculate_person_metrics(algo_predictions, match_results$gt_primary)
# MAGIC person_metrics_all_gt <- calculate_person_metrics(algo_predictions, match_results$gt_all)
# MAGIC
# MAGIC cat("Person-Level (primary eligible GT):\n")
# MAGIC cat("- Exact match:", round(100 * person_metrics_primary$summary$exact_match_rate, 1), "%\n")
# MAGIC cat("- Within +/-1:", round(100 * person_metrics_primary$summary$within_1_rate, 1), "%\n")
# MAGIC cat("- Mean abs diff:", round(person_metrics_primary$summary$mean_abs_diff, 2), "\n\n")
# MAGIC
# MAGIC cat("Person-Level (all GT sensitivity):\n")
# MAGIC cat("- Exact match:", round(100 * person_metrics_all_gt$summary$exact_match_rate, 1), "%\n")
# MAGIC cat("- Within +/-1:", round(100 * person_metrics_all_gt$summary$within_1_rate, 1), "%\n")
# MAGIC cat("- Mean abs diff:", round(person_metrics_all_gt$summary$mean_abs_diff, 2), "\n")
# MAGIC
# MAGIC # Person summary table
# MAGIC tibble::tibble(
# MAGIC   analysis = c("Primary: eligible GT only", "Sensitivity: all GT episodes"),
# MAGIC   exact_match_rate = c(person_metrics_primary$summary$exact_match_rate, person_metrics_all_gt$summary$exact_match_rate),
# MAGIC   within_1_rate = c(person_metrics_primary$summary$within_1_rate, person_metrics_all_gt$summary$within_1_rate),
# MAGIC   mean_abs_diff = c(person_metrics_primary$summary$mean_abs_diff, person_metrics_all_gt$summary$mean_abs_diff)
# MAGIC ) %>%
# MAGIC   dplyr::mutate(dplyr::across(c(exact_match_rate, within_1_rate), ~round(100 * ., 1)), mean_abs_diff = round(mean_abs_diff, 2))
# MAGIC
# MAGIC cat("\nPerson-Level Aggregation Summary:\n")
# MAGIC print(knitr::kable(
# MAGIC   tibble::tibble(
# MAGIC     analysis = c("Primary: eligible GT only", "Sensitivity: all GT episodes"),
# MAGIC     exact_match_rate = c(person_metrics_primary$summary$exact_match_rate, person_metrics_all_gt$summary$exact_match_rate),
# MAGIC     within_1_rate = c(person_metrics_primary$summary$within_1_rate, person_metrics_all_gt$summary$within_1_rate),
# MAGIC     mean_abs_diff = c(person_metrics_primary$summary$mean_abs_diff, person_metrics_all_gt$summary$mean_abs_diff)
# MAGIC   ) %>% dplyr::mutate(dplyr::across(c(exact_match_rate, within_1_rate), ~round(100 * ., 1)), mean_abs_diff = round(mean_abs_diff, 2)),
# MAGIC   format = "simple",
# MAGIC   col.names = c("Analysis", "Exact Match (%)", "Within +/-1 (%)", "Mean Abs Diff")))

# COMMAND ----------

# DBTITLE 1,complications-query
# MAGIC %r
# MAGIC # Pregnancy complications: setup + descendants + condition query
# MAGIC complication_concepts <- tibble::tibble(
# MAGIC   complication = c("Preeclampsia", "Pregnancy-induced hypertension", "Gestational diabetes"),
# MAGIC   concept_id = c(439393, 4167493, 4024659),
# MAGIC   short_name = c("preeclampsia", "pih", "gdm")
# MAGIC )
# MAGIC
# MAGIC # Reopen connection
# MAGIC con <- open_report_connection()
# MAGIC stopifnot(!is.null(con), db_is_valid(con))
# MAGIC cat("Database connection established\n")
# MAGIC
# MAGIC ensure_connection <- function(conn) {
# MAGIC   ok <- tryCatch(db_is_valid(conn), error = function(e) FALSE)
# MAGIC   if (!ok) stop("Database connection invalid. Re-run db-connect.")
# MAGIC   invisible(conn)
# MAGIC }
# MAGIC
# MAGIC # Get descendant concept IDs
# MAGIC get_complication_descendants <- function(conn, complication_concepts) {
# MAGIC   ensure_connection(conn)
# MAGIC   ancestor_table <- paste0(params$omop_database, ".", params$omop_schema, ".concept_ancestor")
# MAGIC   out_list <- lapply(seq_len(nrow(complication_concepts)), function(i) {
# MAGIC     aid <- complication_concepts$concept_id[i]; sn <- complication_concepts$short_name[i]
# MAGIC     sql <- paste0("SELECT DISTINCT ", aid, " AS ancestor_concept_id, '", sn, "' AS short_name, descendant_concept_id FROM ",
# MAGIC                   ancestor_table, " WHERE ancestor_concept_id = ", aid,
# MAGIC                   " UNION SELECT DISTINCT ", aid, " AS ancestor_concept_id, '", sn, "' AS short_name, ", aid, " AS descendant_concept_id")
# MAGIC     db_query(conn, sql) %>% tibble::as_tibble()
# MAGIC   })
# MAGIC   dplyr::bind_rows(out_list) %>% dplyr::distinct()
# MAGIC }
# MAGIC
# MAGIC complication_descendants <- get_complication_descendants(con, complication_concepts)
# MAGIC cat("Descendant concept rows:", nrow(complication_descendants), "\n")
# MAGIC cat("Unique descendant IDs:", dplyr::n_distinct(complication_descendants$descendant_concept_id), "\n")
# MAGIC
# MAGIC # Query condition_occurrence in batches
# MAGIC query_complication_conditions <- function(conn, person_ids, complication_descendants, batch_size = 5000) {
# MAGIC   ensure_connection(conn)
# MAGIC   condition_table <- paste0(params$omop_database, ".", params$omop_schema, ".condition_occurrence")
# MAGIC   person_ids <- unique(person_ids[!is.na(person_ids)])
# MAGIC   concept_ids <- unique(complication_descendants$descendant_concept_id)
# MAGIC   concept_sql <- paste(concept_ids, collapse = ", ")
# MAGIC   person_batches <- split(person_ids, ceiling(seq_along(person_ids) / batch_size))
# MAGIC   out_list <- lapply(seq_along(person_batches), function(i) {
# MAGIC     person_sql <- paste(person_batches[[i]], collapse = ", ")
# MAGIC     sql <- paste0("SELECT person_id, CAST(condition_start_date AS DATE) AS condition_start_date, condition_concept_id FROM ",
# MAGIC                   condition_table, " WHERE condition_concept_id IN (", concept_sql, ") AND person_id IN (", person_sql, ") AND condition_concept_id <> 0")
# MAGIC     cat("Batch", i, "of", length(person_batches), "...\n")
# MAGIC     db_query(conn, sql) %>% tibble::as_tibble()
# MAGIC   })
# MAGIC   dplyr::bind_rows(out_list) %>%
# MAGIC     dplyr::mutate(condition_start_date = as.Date(condition_start_date)) %>%
# MAGIC     dplyr::inner_join(complication_descendants, by = c("condition_concept_id" = "descendant_concept_id"), relationship = "many-to-many") %>%
# MAGIC     dplyr::distinct(person_id, condition_start_date, condition_concept_id, ancestor_concept_id, short_name)
# MAGIC }
# MAGIC
# MAGIC persons_of_interest <- unique(c(unique(match_results$gt_all$person_id), unique(algo_predictions$person_id)))
# MAGIC complication_conditions <- query_complication_conditions(con, persons_of_interest, complication_descendants, 5000)
# MAGIC cat("\nTotal complication records:", nrow(complication_conditions), "\n")
# MAGIC cat("Unique persons with complications:", dplyr::n_distinct(complication_conditions$person_id), "\n")

# COMMAND ----------

# DBTITLE 1,complications-flags
# MAGIC %r
# MAGIC # Episode-level complication flags + prevalence + agreement
# MAGIC flag_episode_complications <- function(episodes, person_col, episode_col, start_col, end_col, conditions) {
# MAGIC   ep <- episodes %>%
# MAGIC     dplyr::transmute(person_id = .data[[person_col]], episode_id = .data[[episode_col]],
# MAGIC                      episode_start = as.Date(.data[[start_col]]), episode_end = as.Date(.data[[end_col]])) %>%
# MAGIC     dplyr::mutate(episode_start = dplyr::coalesce(episode_start, episode_end),
# MAGIC                   episode_end = dplyr::coalesce(episode_end, episode_start),
# MAGIC                   episode_key = paste(person_id, episode_id, sep = "::")) %>%
# MAGIC     dplyr::filter(!is.na(person_id), !is.na(episode_start), !is.na(episode_end))
# MAGIC   if (nrow(ep) == 0) return(tibble::tibble())
# MAGIC   cond <- conditions %>% dplyr::filter(!is.na(person_id), !is.na(condition_start_date), !is.na(short_name)) %>%
# MAGIC     dplyr::distinct(person_id, condition_start_date, condition_concept_id, short_name)
# MAGIC   if (nrow(cond) == 0) return(ep %>% dplyr::transmute(episode_key, preeclampsia_flag = 0L, pih_flag = 0L, gdm_flag = 0L, any_complication_flag = 0L))
# MAGIC   episode_hits_long <- ep %>%
# MAGIC     dplyr::inner_join(cond, by = "person_id", relationship = "many-to-many") %>%
# MAGIC     dplyr::filter(condition_start_date >= episode_start, condition_start_date <= episode_end) %>%
# MAGIC     dplyr::distinct(episode_key, short_name)
# MAGIC   flags_wide <- episode_hits_long %>% dplyr::mutate(flag = 1L) %>%
# MAGIC     tidyr::pivot_wider(names_from = short_name, values_from = flag, values_fill = 0)
# MAGIC   ep %>% dplyr::distinct(episode_key) %>%
# MAGIC     dplyr::left_join(flags_wide, by = "episode_key") %>%
# MAGIC     dplyr::mutate(preeclampsia = dplyr::coalesce(preeclampsia, 0L), pih = dplyr::coalesce(pih, 0L), gdm = dplyr::coalesce(gdm, 0L)) %>%
# MAGIC     dplyr::transmute(episode_key, preeclampsia_flag = as.integer(preeclampsia > 0), pih_flag = as.integer(pih > 0),
# MAGIC                      gdm_flag = as.integer(gdm > 0), any_complication_flag = as.integer((preeclampsia > 0) | (pih > 0) | (gdm > 0)))
# MAGIC }
# MAGIC
# MAGIC algo_complication_flags <- flag_episode_complications(algo_predictions, "person_id", "episode_number", "episode_start_date", "episode_end_date", complication_conditions)
# MAGIC gt_primary_complication_flags <- flag_episode_complications(match_results$gt_primary, "person_id", "gt_episode_num", "gt_start", "gt_end", complication_conditions)
# MAGIC gt_all_complication_flags <- flag_episode_complications(match_results$gt_all, "person_id", "gt_episode_num", "gt_start", "gt_end", complication_conditions)
# MAGIC
# MAGIC cat("Complication flags: algo=", nrow(algo_complication_flags), ", GT primary=", nrow(gt_primary_complication_flags), ", GT all=", nrow(gt_all_complication_flags), "\n")
# MAGIC
# MAGIC # Prevalence
# MAGIC summarise_complication_flags <- function(flags, label) {
# MAGIC   flags %>% dplyr::summarise(preeclampsia = mean(preeclampsia_flag, na.rm = TRUE), pih = mean(pih_flag, na.rm = TRUE),
# MAGIC     gdm = mean(gdm_flag, na.rm = TRUE), any_complication = mean(any_complication_flag, na.rm = TRUE)) %>%
# MAGIC     dplyr::mutate(approach = label) %>% dplyr::select(approach, preeclampsia, pih, gdm, any_complication)
# MAGIC }
# MAGIC
# MAGIC cat("\nPrevalence Summary:\n")
# MAGIC dplyr::bind_rows(
# MAGIC   summarise_complication_flags(algo_complication_flags, "Algorithm"),
# MAGIC   summarise_complication_flags(gt_primary_complication_flags, "Eligible GT"),
# MAGIC   summarise_complication_flags(gt_all_complication_flags, "All GT")
# MAGIC ) %>% dplyr::mutate(dplyr::across(c(preeclampsia, pih, gdm, any_complication), ~round(100 * ., 1)))
# MAGIC
# MAGIC print(knitr::kable(
# MAGIC   dplyr::bind_rows(
# MAGIC     summarise_complication_flags(algo_complication_flags, "Algorithm"),
# MAGIC     summarise_complication_flags(gt_primary_complication_flags, "Eligible GT"),
# MAGIC     summarise_complication_flags(gt_all_complication_flags, "All GT")
# MAGIC   ) %>% dplyr::mutate(dplyr::across(c(preeclampsia, pih, gdm, any_complication), ~round(100 * ., 1))),
# MAGIC   format = "simple", col.names = c("Approach", "Preeclampsia", "PIH", "GDM", "Any")))
# MAGIC
# MAGIC # Agreement on matched episodes
# MAGIC matched_complications <- matched_episodes %>%
# MAGIC   dplyr::left_join(algo_complication_flags %>% dplyr::rename(algo_preeclampsia_flag = preeclampsia_flag, algo_pih_flag = pih_flag,
# MAGIC     algo_gdm_flag = gdm_flag, algo_any_complication_flag = any_complication_flag), by = c("algo_key" = "episode_key")) %>%
# MAGIC   dplyr::left_join(gt_primary_complication_flags %>% dplyr::rename(gt_preeclampsia_flag = preeclampsia_flag, gt_pih_flag = pih_flag,
# MAGIC     gt_gdm_flag = gdm_flag, gt_any_complication_flag = any_complication_flag), by = c("gt_key" = "episode_key")) %>%
# MAGIC   dplyr::mutate(dplyr::across(c(algo_preeclampsia_flag, algo_pih_flag, algo_gdm_flag, algo_any_complication_flag,
# MAGIC     gt_preeclampsia_flag, gt_pih_flag, gt_gdm_flag, gt_any_complication_flag), ~dplyr::coalesce(., 0L)))
# MAGIC
# MAGIC calculate_binary_comparison_metrics <- function(dat, algo_col, gt_col, label) {
# MAGIC   a <- dat[[algo_col]]; g <- dat[[gt_col]]
# MAGIC   tp <- sum(a == 1 & g == 1, na.rm = TRUE); fp <- sum(a == 1 & g == 0, na.rm = TRUE)
# MAGIC   fn <- sum(a == 0 & g == 1, na.rm = TRUE); tn <- sum(a == 0 & g == 0, na.rm = TRUE); n <- tp + fp + fn + tn
# MAGIC   sens <- ifelse(tp + fn > 0, tp/(tp+fn), NA); spec <- ifelse(tn + fp > 0, tn/(tn+fp), NA)
# MAGIC   ppv <- ifelse(tp + fp > 0, tp/(tp+fp), NA); agree <- ifelse(n > 0, (tp+tn)/n, NA)
# MAGIC   p_exp <- ifelse(n > 0, ((tp+fp)/n)*((tp+fn)/n) + ((fn+tn)/n)*((fp+tn)/n), NA)
# MAGIC   kappa <- ifelse(!is.na(p_exp) && (1-p_exp) > 0, (agree - p_exp)/(1 - p_exp), NA)
# MAGIC   tibble::tibble(complication = label, n = n, algo_pos = sum(a==1), gt_pos = sum(g==1), tp = tp, fp = fp, fn = fn,
# MAGIC                  sensitivity = sens, specificity = spec, ppv = ppv, kappa = kappa)
# MAGIC }
# MAGIC
# MAGIC cat("\nAgreement (N=", nrow(matched_complications), "matched episodes):\n")
# MAGIC dplyr::bind_rows(
# MAGIC   calculate_binary_comparison_metrics(matched_complications, "algo_preeclampsia_flag", "gt_preeclampsia_flag", "Preeclampsia"),
# MAGIC   calculate_binary_comparison_metrics(matched_complications, "algo_pih_flag", "gt_pih_flag", "PIH"),
# MAGIC   calculate_binary_comparison_metrics(matched_complications, "algo_gdm_flag", "gt_gdm_flag", "GDM"),
# MAGIC   calculate_binary_comparison_metrics(matched_complications, "algo_any_complication_flag", "gt_any_complication_flag", "Any")
# MAGIC ) %>% dplyr::mutate(dplyr::across(c(sensitivity, specificity, ppv), ~round(100*., 1)), kappa = round(kappa, 3))
# MAGIC
# MAGIC print(knitr::kable(
# MAGIC   dplyr::bind_rows(
# MAGIC     calculate_binary_comparison_metrics(matched_complications, "algo_preeclampsia_flag", "gt_preeclampsia_flag", "Preeclampsia"),
# MAGIC     calculate_binary_comparison_metrics(matched_complications, "algo_pih_flag", "gt_pih_flag", "PIH"),
# MAGIC     calculate_binary_comparison_metrics(matched_complications, "algo_gdm_flag", "gt_gdm_flag", "GDM"),
# MAGIC     calculate_binary_comparison_metrics(matched_complications, "algo_any_complication_flag", "gt_any_complication_flag", "Any")
# MAGIC   ) %>% dplyr::mutate(dplyr::across(c(sensitivity, specificity, ppv), ~round(100*., 1)), kappa = round(kappa, 3)),
# MAGIC   format = "simple",
# MAGIC   col.names = c("Complication", "N", "Algo+", "GT+", "TP", "FP", "FN", "Sens(%)", "Spec(%)", "PPV(%)", "Kappa")))

# COMMAND ----------

# DBTITLE 1,summary-and-cleanup
# MAGIC %r
# MAGIC # Overall validation summary + cleanup
# MAGIC summary_df <- tibble::tibble(
# MAGIC   Metric = c("Algorithm episodes", "Ground truth episodes (all)", "Eligible GT episodes",
# MAGIC              "Historical GT removed", "Primary matched (overlap)",
# MAGIC              "Algorithm match rate (primary)", "GT match rate (primary)",
# MAGIC              "Algorithm match rate (all GT)", "All-GT match rate",
# MAGIC              "Mean GA difference (days)", "GA within +/-7 days", "GA within +/-14 days",
# MAGIC              "Person exact count match (eligible GT)", "Person within +/-1 episode"),
# MAGIC   Value = c(match_results$primary_overlap$stats$total_algo,
# MAGIC             match_results$all_gt_overlap$stats$total_gt,
# MAGIC             match_results$primary_overlap$stats$total_gt,
# MAGIC             match_results$historical_summary$likely_historical_gt_episodes,
# MAGIC             match_results$primary_overlap$stats$matched,
# MAGIC             round(100 * match_results$primary_overlap$stats$match_rate_algo, 1),
# MAGIC             round(100 * match_results$primary_overlap$stats$match_rate_gt, 1),
# MAGIC             round(100 * match_results$all_gt_overlap$stats$match_rate_algo, 1),
# MAGIC             round(100 * match_results$all_gt_overlap$stats$match_rate_gt, 1),
# MAGIC             ifelse(nrow(ga_metrics$summary) > 0, round(ga_metrics$summary$mean_diff_days, 1), NA_real_),
# MAGIC             ifelse(nrow(ga_metrics$summary) > 0, round(100 * ga_metrics$summary$within_7_days, 1), NA_real_),
# MAGIC             ifelse(nrow(ga_metrics$summary) > 0, round(100 * ga_metrics$summary$within_14_days, 1), NA_real_),
# MAGIC             round(100 * person_metrics_primary$summary$exact_match_rate, 1),
# MAGIC             round(100 * person_metrics_primary$summary$within_1_rate, 1))
# MAGIC )
# MAGIC
# MAGIC cat("=== OVERALL VALIDATION SUMMARY ===\n")
# MAGIC print(knitr::kable(summary_df, format = "simple"))
# MAGIC
# MAGIC # --- Outcome-stratified matching summary ---
# MAGIC gt_primary_sum <- match_results$gt_primary
# MAGIC matched_gt_keys_sum <- matched_episodes %>% dplyr::pull(gt_key)
# MAGIC algo_matched_keys_sum <- matched_episodes %>% dplyr::pull(algo_key)
# MAGIC
# MAGIC gt_strat <- gt_primary_sum %>%
# MAGIC   dplyr::mutate(gt_key = paste(person_id, gt_episode_num, sep = "::"),
# MAGIC                 matched = gt_key %in% matched_gt_keys_sum) %>%
# MAGIC   dplyr::group_by(gt_outcome) %>%
# MAGIC   dplyr::summarise(gt_total = dplyr::n(), gt_matched = sum(matched),
# MAGIC                    gt_match_rate = round(100 * mean(matched), 1), .groups = "drop") %>%
# MAGIC   dplyr::arrange(dplyr::desc(gt_total))
# MAGIC
# MAGIC algo_strat <- algo_predictions %>%
# MAGIC   dplyr::mutate(algo_key = paste(person_id, episode_number, sep = "::"),
# MAGIC                 matched = algo_key %in% algo_matched_keys_sum) %>%
# MAGIC   dplyr::group_by(outcome_category) %>%
# MAGIC   dplyr::summarise(algo_total = dplyr::n(), algo_matched = sum(matched),
# MAGIC                    algo_match_rate = round(100 * mean(matched), 1), .groups = "drop") %>%
# MAGIC   dplyr::arrange(dplyr::desc(algo_total))
# MAGIC
# MAGIC cat("\n=== OUTCOME-STRATIFIED MATCHING ===\n\n")
# MAGIC cat("GT side (match rate = % of eligible GT episodes matched by algorithm):\n")
# MAGIC print(knitr::kable(gt_strat, format = "simple",
# MAGIC   col.names = c("GT Outcome", "Total", "Matched", "Match Rate (%)")))
# MAGIC
# MAGIC cat("\nAlgorithm side (match rate = % of algo episodes matched to GT):\n")
# MAGIC print(knitr::kable(algo_strat, format = "simple",
# MAGIC   col.names = c("Algo Outcome", "Total", "Matched", "Match Rate (%)")))
# MAGIC
# MAGIC cat("\nKey Findings:\n")
# MAGIC cat("1.", match_results$primary_overlap$stats$matched, "episodes matched via overlap (primary).\n")
# MAGIC cat("2. Algorithm match rate:", round(100*match_results$primary_overlap$stats$match_rate_algo, 1), "%;")
# MAGIC cat(" GT match rate:", round(100*match_results$primary_overlap$stats$match_rate_gt, 1), "%.\n")
# MAGIC cat("   - LB GT match rate:", gt_strat$gt_match_rate[gt_strat$gt_outcome == "LB"], "%;")
# MAGIC cat(" SB:", gt_strat$gt_match_rate[gt_strat$gt_outcome == "SB"], "%;")
# MAGIC cat(" SA:", gt_strat$gt_match_rate[gt_strat$gt_outcome == "SA"], "%;")
# MAGIC cat(" AB:", gt_strat$gt_match_rate[gt_strat$gt_outcome == "AB"], "%\n")
# MAGIC if (nrow(ga_metrics$summary) > 0) {
# MAGIC   cat("3. Mean GA diff:", round(ga_metrics$summary$mean_diff_days, 1), "d;")
# MAGIC   cat(" within +/-14d:", round(100*ga_metrics$summary$within_14_days, 1), "%.\n")
# MAGIC }
# MAGIC cat("4. Outcome accuracy:", round(100*class_metrics$overall_accuracy, 1), "%.\n")
# MAGIC cat("5. Person-level exact match:", round(100*person_metrics_primary$summary$exact_match_rate, 1), "%.\n")
# MAGIC
# MAGIC # Cleanup
# MAGIC tryCatch(db_disconnect(con), error = function(e) cat("Note: disconnect:", conditionMessage(e), "\n"))
# MAGIC cat("\nDone. Connection closed.\n")
