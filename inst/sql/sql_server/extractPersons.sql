-- Extract persons of reproductive age who are female
SELECT DISTINCT
  p.person_id,
  p.gender_concept_id,
  p.year_of_birth,
  p.month_of_birth,
  p.day_of_birth,
  p.race_concept_id,
  p.ethnicity_concept_id
FROM @cdm_schema.person p
WHERE p.gender_concept_id IN (8532, 8507, 45878463)  -- Female gender concepts
  AND p.year_of_birth >= YEAR(GETDATE()) - @max_age
  AND p.year_of_birth <= YEAR(GETDATE()) - @min_age;