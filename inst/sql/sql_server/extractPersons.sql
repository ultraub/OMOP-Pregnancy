-- Extract persons of reproductive age who are not recorded as male
SELECT DISTINCT
  p.person_id,
  p.gender_concept_id,
  p.year_of_birth,
  p.month_of_birth,
  p.day_of_birth,
  p.race_concept_id,
  p.ethnicity_concept_id
FROM @cdm_schema.person p
WHERE p.gender_concept_id NOT IN (@male_concept_ids)  -- 8507 = MALE
  AND p.year_of_birth >= YEAR(GETDATE()) - @max_age
  AND p.year_of_birth <= YEAR(GETDATE()) - @min_age;