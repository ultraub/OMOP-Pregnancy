source("tests/helpers.R"); ok <- TRUE
run_hip <- function(c) suppressMessages(run_hip_algorithm(c, concepts$matcho_limits, concepts$matcho_outcome_limits))
# Distinct outcome categories must not collide on episode_number (issue 2)
o <- run_hip(cohort_of(bind_rows(rec(1L, 4014295L, "2018-09-01", "Single live birth", "LB"),
  rec(1L, 4185780L, "2018-04-20", "Gestation period, 21 weeks", "PREG", 21), rec(1L, 444098L, "2018-08-30", "Gestation period, 40 weeks", "PREG", 40),
  rec(1L, 4067106L, "2020-06-15", "Spontaneous abortion", "SA"))))
ok <- check("LB and SA both survive gestation matching", nrow(o) == 2 && setequal(o$outcome_category, c("LB", "SA"))) && ok
ok <- check("start anchored on the GA record (Aug 30 - 280 days)", o$episode_start_date[o$outcome_category == "LB"] == d("2017-11-23")) && ok
# Start anchoring with a 30-week code ten weeks before delivery (issue 3)
o <- run_hip(cohort_of(bind_rows(rec(2L, 4014295L, "2019-08-10", "Single live birth", "LB"), rec(2L, 4059741L, "2019-06-01", "Gestation period, 30 weeks", "PREG", 30))))
ok <- check("30-week record anchors start at record date - 210", o$episode_start_date == d("2018-11-03")) && ok
# Revised dating is one gestation episode (issue 9)
o <- run_hip(cohort_of(bind_rows(rec(4L, 4185780L, "2021-03-01", "Gestation period, 20 weeks", "PREG", 20), rec(4L, 4049621L, "2021-04-10", "Gestation period, 18 weeks", "PREG", 18))))
ok <- check("20w then 18w 40 days later is one PREG episode", nrow(o) == 1 && o$outcome_category == "PREG") && ok
# Gestation-only PREG over 301 days is kept with clean flags (issue 10)
o <- run_hip(cohort_of(bind_rows(rec(5L, 4220085L, "2019-01-01", "Gestation period, 2 weeks", "PREG", 2), rec(5L, 444098L, "2019-10-24", "Gestation period, 44 weeks", "PREG", 44))))
ok <- check("long gestation-only PREG kept, not flagged", nrow(o) == 1 && o$removed_outcome == 0 && is.na(o$removed_category) && o$gestational_age_days > 301) && ok
# Touching episodes do not shift; overlapping ones shift by the previous retry (issue 10)
o <- run_hip(cohort_of(bind_rows(rec(6L, 4014295L, "2018-03-01", "Single live birth", "LB"), rec(6L, 4067106L, "2018-07-18", "Spontaneous abortion", "SA"))))
ok <- check("SA starting exactly at LB end is not shifted", o$episode_start_date[o$outcome_category == "SA"] == d("2018-03-01")) && ok
o <- run_hip(cohort_of(bind_rows(rec(7L, 4014295L, "2018-03-01", "Single live birth", "LB"), rec(7L, 4067106L, "2018-06-09", "Spontaneous abortion", "SA"))))
ok <- check("overlapping SA shifted to LB end + 28", o$episode_start_date[o$outcome_category == "SA"] == d("2018-03-29")) && ok
# first_gest_date present and NA without gestation records
ok <- check("first_gest_date NA without gestation data", "first_gest_date" %in% names(o) && all(is.na(o$first_gest_date))) && ok
if (!ok) quit(status = 1)
