source("tests/helpers.R"); ok <- TRUE
timing <- bind_rows(gt(1L, 4260747L, "2020-01-01", 1, 2), gt(1L, 3009306L, "2020-04-20", 3.75, 5.5), gt(2L, 4260747L, "2021-03-10", 1, 2), gt(2L, 3016670L, "2021-05-01", 3.75, 5.5))
conds <- rec(1L, 4014295L, "2020-11-20", "Single live birth", "LB")
cohort <- cohort_of(conds, timing)
pps <- suppressMessages(run_pps_algorithm(cohort, concepts$pps_concepts))
ok <- check("PPS: long but valid episode kept, outcome LB", nrow(pps) == 2 && pps$outcome_category[pps$person_id == 1] == "LB") && ok
ok <- check("PPS: no-outcome episode has NA outcome and end = last concept", is.na(pps$outcome_category[pps$person_id == 2]) && pps$episode_end_date[pps$person_id == 2] == d("2021-05-01")) && ok
ok <- check("PPS: end dates keep Date class", inherits(pps$episode_end_date, "Date")) && ok
hip <- data.frame(person_id = c(1L, 2L), episode_number = 1L, episode_start_date = d(c("2020-02-14", "2021-02-01")), episode_end_date = d(c("2020-11-20", "2021-05-01")),
  outcome_category = c("LB", "PREG"), gestational_age_days = c(280, 89), has_gestational_info = TRUE, first_gest_date = d(c("2020-09-01", NA)), algorithm_used = "HIP")
m <- suppressMessages(merge_pregnancy_episodes(hip, pps))
ok <- check("merge: both episodes MERGED, nothing dropped", nrow(m) == 2 && all(m$algorithm_used == "MERGED")) && ok
ok <- check("merge: recorded start is the first PPS concept (Jan 1)", m$recorded_episode_start[m$person_id == 1] == d("2020-01-01")) && ok
ok <- check("merge: PPS no-outcome becomes PREG and matches HIP PREG", m$PPS_outcome_category[m$person_id == 2] == "PREG" && m$outcome_category[m$person_id == 2] == "PREG") && ok
ok <- check("merge: HIP-only and PPS-only paths work", nrow(suppressMessages(merge_pregnancy_episodes(hip, pps[0, ]))) == 2 && nrow(suppressMessages(merge_pregnancy_episodes(hip[0, ], pps))) == 2) && ok
# Duplicate resolution keeps the losers as single-algorithm rows (N3C)
hipf <- function(pid, n, s, e, cat) data.frame(person_id = pid, episode_number = n, episode_start_date = d(s), episode_end_date = d(e), outcome_category = cat,
  gestational_age_days = as.numeric(d(e) - d(s)), has_gestational_info = FALSE, first_gest_date = d(NA), algorithm_used = "HIP")
ppsf <- function(pid, n, mn, mx, cat, od) data.frame(person_id = pid, episode_number = n, episode_min_date = d(mn), episode_max_date = d(mx), outcome_category = cat,
  outcome_date = d(od), episode_start_date = d(mn), episode_end_date = d(ifelse(is.na(od), mx, od)), n_GT_concepts = 2L, n_records = 3L, algorithm_used = "PPS")
m1 <- suppressMessages(merge_pregnancy_episodes(bind_rows(hipf(1L, 1L, "2020-01-10", "2020-03-01", "SA"), hipf(1L, 2L, "2020-04-20", "2021-01-15", "LB")), ppsf(1L, 1L, "2020-02-15", "2021-01-10", "LB", "2021-01-15")))
ok <- check("dedup: HIP SA losing a PPS tie survives as HIP-only", nrow(m1) == 2 && any(m1$outcome_category == "SA" & m1$algorithm_used == "HIP")) && ok
m2 <- suppressMessages(merge_pregnancy_episodes(hipf(2L, 1L, "2019-03-01", "2019-12-01", "LB"), bind_rows(ppsf(2L, 1L, "2019-02-20", "2019-03-10", NA, NA), ppsf(2L, 2L, "2019-05-01", "2019-11-28", "LB", "2019-12-01"))))
ok <- check("dedup: PPS episode losing a HIP tie survives as PPS-only", nrow(m2) == 2 && any(m2$algorithm_used == "PPS")) && ok
# ESD + metadata end to end
esd <- suppressMessages(calculate_estimated_start_dates(m, cohort, concepts$pps_concepts)); meta <- add_episode_quality_metadata(esd, concepts$matcho_limits)
ok <- check("ESD: range-only episode gets a midpoint start", !is.na(esd$inferred_episode_start[esd$person_id == 2])) && ok
ok <- check("metadata: every episode has a start and precision category", all(!is.na(meta$inferred_episode_start)) && all(!is.na(meta$precision_category))) && ok
ok <- check("metadata: no working columns leak", !any(c("GT_type", "domain_name") %in% names(meta))) && ok
ok <- check("nothing masks dplyr::coalesce when all files are sourced", identical(environment(coalesce), asNamespace("dplyr"))) && ok
if (!ok) quit(status = 1)
