source("tests/helpers.R"); ok <- TRUE
esd <- function(cid, name, date, van = NA_real_, vas = NA_character_) data.frame(person_id = 1L, concept_id = cid, concept_name = name, event_date = d(date), domain_name = "x", value_as_number = van, value_as_string = vas)
ev <- bind_rows(esd(4059741L, "Gestation period, 30 weeks", "2020-09-01"), esd(4059738L, "Gestation period, 28 weeks", "2020-09-01"),
  esd(3048230L, "Gestational age", "2020-05-10", van = 12.5), esd(3048230L, "Gestational age", "2020-05-20", van = 50),
  esd(3048230L, "Gestational age", "2020-05-25", vas = "|text_result_val:14"), esd(3009306L, "Alpha-1-Fetoprotein [Mass/volume] in Serum or Plasma", "2020-06-15"),
  esd(4059741L, "Gestation period, 30 weeks", "2020-11-30"))
episodes <- data.frame(person_id = 1L, episode_number = 1L, episode_start_date = d("2020-02-01"), episode_end_date = d("2020-11-20"), recorded_episode_end = d("2020-11-20"))
out <- get_timing_concepts(episodes, list(esd_timing = ev), concepts$pps_concepts)
ok <- check("one row per date for week evidence, highest week kept", sum(out$GT_type == "GW") == 3 && out$gestational_weeks[out$event_date == d("2020-09-01")] == 30) && ok
ok <- check("shared HIP/PPS week code is GW only, not also a range", sum(out$GT_type == "GR3m") == 1) && ok
ok <- check("value truncated, out-of-range value dropped, string value parsed", out$gestational_weeks[out$event_date == d("2020-05-10")] == 12 && !any(out$event_date == d("2020-05-20")) && out$gestational_weeks[out$event_date == d("2020-05-25")] == 14) && ok
ok <- check("record after the recorded end is excluded", !any(out$event_date == d("2020-11-30"))) && ok
ok <- check("week rows ordered by date", !is.unsorted(out$event_date[out$GT_type == "GW"])) && ok
# Interval intersection and combiner: reference behaviour on hand cases
ci <- findIntersection(list(c(d("2020-01-01"), d("2020-01-10")), c(d("2020-01-05"), d("2020-01-20")), c(d("2020-01-02"), d("2020-01-04"))))
ok <- check("findIntersection: sequential narrowing (Jan 5 - Jan 10)", ci[4] == d("2020-01-05") && ci[3] == d("2020-01-10")) && ok
r <- find_timing_intersection(data.frame(implied_start_date = d(character(0)), GT_type = character(0)), data.frame(range_start = d(c("2020-01-01", "2020-01-05")), range_end = d(c("2020-01-10", "2020-01-20"))))
ok <- check("range-only: midpoint start, union-span precision (19 days)", r$precision_days == 19 && r$inferred_start_date == d("2020-01-07")) && ok
r <- find_timing_intersection(data.frame(implied_start_date = d("2020-03-01"), GT_type = "GW"), data.frame(range_start = d(character(0)), range_end = d(character(0))))
ok <- check("single week estimate: week_poor-support", r$precision_days == -1 && r$precision_category == "week_poor-support") && ok
if (!ok) quit(status = 1)
