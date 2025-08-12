-- OMOP Concept Validation Script
-- Run this against your OMOP CDM to validate pregnancy-related concepts
-- Database: SQL Server/PostgreSQL compatible

-- Check database connection and CDM version
SELECT 
    'CDM Version Check' as check_type,
    cdm_version,
    cdm_release_date,
    cdm_version_concept_id,
    vocabulary_version
FROM cdm_source
LIMIT 1;

-- Validate Gender/Sex Concepts
WITH gender_concepts AS (
    SELECT 8507 as concept_id, 'Male' as expected_name
    UNION ALL SELECT 8532, 'Female'
    UNION ALL SELECT 45880669, 'All of Us Male'
    UNION ALL SELECT 45878463, 'All of Us Female'
)
SELECT 
    'Gender Concepts' as check_type,
    gc.concept_id as expected_concept_id,
    gc.expected_name,
    c.concept_id as found_concept_id,
    c.concept_name,
    c.domain_id,
    c.vocabulary_id,
    c.concept_class_id,
    c.standard_concept,
    CASE WHEN c.concept_id IS NULL THEN 'MISSING' 
         WHEN c.invalid_reason IS NOT NULL THEN 'INVALID'
         ELSE 'VALID' END as status
FROM gender_concepts gc
LEFT JOIN concept c ON gc.concept_id = c.concept_id;

-- Validate Gestational Age Observation Concepts
WITH gestational_obs_concepts AS (
    SELECT concept_id, expected_name FROM (
        VALUES 
        (3011536, 'Estimated date of delivery'),
        (3026070, 'Date of last menstrual period'),
        (3024261, 'Pregnancy test'),
        (4260747, 'Gestational age at delivery'),
        (40758410, 'Gestational age at scan'),
        (3002549, 'Expected date of delivery'),
        (43054890, 'Gestational age by scan'),
        (46234792, 'Gestational age at event'),
        (4266763, 'Gestational age at birth'),
        (40485048, 'Clinical gestational age'),
        (3048230, 'Gestational age in weeks'),
        (3002209, 'Gestational age Estimated'),
        (3012266, 'Gestational age')
    ) AS t(concept_id, expected_name)
)
SELECT 
    'Gestational Age Obs' as check_type,
    goc.concept_id as expected_concept_id,
    goc.expected_name,
    c.concept_id as found_concept_id,
    c.concept_name,
    c.domain_id,
    c.vocabulary_id,
    c.standard_concept,
    CASE WHEN c.concept_id IS NULL THEN 'MISSING'
         WHEN c.domain_id NOT IN ('Observation', 'Measurement', 'Condition') THEN 'WRONG_DOMAIN'
         WHEN c.invalid_reason IS NOT NULL THEN 'INVALID'
         ELSE 'VALID' END as status
FROM gestational_obs_concepts goc
LEFT JOIN concept c ON goc.concept_id = c.concept_id
ORDER BY goc.concept_id;

-- Validate Pregnancy Outcome Concepts  
WITH outcome_concepts AS (
    SELECT concept_id, outcome_type, expected_name FROM (
        VALUES
        -- Live births
        (4092289, 'Live Birth', 'Live birth'),
        (4013886, 'Live Birth', 'Live birth of infant'),
        (4012732, 'Live Birth', 'Full term live birth'),
        -- Stillbirths
        (4079978, 'Stillbirth', 'Stillbirth'),
        (4013503, 'Stillbirth', 'Fetal death'),
        (435559, 'Stillbirth', 'Intrauterine death'),
        -- Spontaneous abortion
        (4067106, 'Spontaneous Abortion', 'Miscarriage'),
        (432303, 'Spontaneous Abortion', 'Spontaneous abortion'),
        (4066746, 'Spontaneous Abortion', 'Complete miscarriage'),
        -- Induced abortion
        (4128331, 'Induced Abortion', 'Termination of pregnancy'),
        (4053936, 'Induced Abortion', 'Induced abortion'),
        (4236484, 'Induced Abortion', 'Medical abortion'),
        -- Ectopic pregnancy
        (433260, 'Ectopic', 'Ectopic pregnancy'),
        (4098004, 'Ectopic', 'Tubal pregnancy'),
        (198874, 'Ectopic', 'Ovarian pregnancy'),
        -- Delivery
        (4081422, 'Delivery', 'Delivery'),
        (4136974, 'Delivery', 'Normal delivery'),
        (40485567, 'Delivery', 'Delivery procedure')
    ) AS t(concept_id, outcome_type, expected_name)
)
SELECT 
    'Pregnancy Outcomes' as check_type,
    oc.outcome_type,
    oc.concept_id as expected_concept_id,
    oc.expected_name,
    c.concept_id as found_concept_id,
    c.concept_name,
    c.domain_id,
    c.vocabulary_id,
    c.standard_concept,
    CASE WHEN c.concept_id IS NULL THEN 'MISSING'
         WHEN c.invalid_reason IS NOT NULL THEN 'INVALID'
         ELSE 'VALID' END as status
FROM outcome_concepts oc
LEFT JOIN concept c ON oc.concept_id = c.concept_id
ORDER BY oc.outcome_type, oc.concept_id;

-- Check for alternative pregnancy-related concepts we might use
SELECT 
    'Alternative Pregnancy Concepts' as check_type,
    c.concept_id,
    c.concept_name,
    c.domain_id,
    c.vocabulary_id,
    c.concept_class_id,
    c.standard_concept
FROM concept c
WHERE c.standard_concept = 'S'
    AND c.invalid_reason IS NULL
    AND (
        LOWER(c.concept_name) LIKE '%pregnan%'
        OR LOWER(c.concept_name) LIKE '%gestation%'
        OR LOWER(c.concept_name) LIKE '%delivery%'
        OR LOWER(c.concept_name) LIKE '%birth%'
        OR LOWER(c.concept_name) LIKE '%abortion%'
        OR LOWER(c.concept_name) LIKE '%miscarriage%'
        OR LOWER(c.concept_name) LIKE '%ectopic%'
    )
    AND c.domain_id IN ('Condition', 'Observation', 'Procedure', 'Measurement')
LIMIT 100;

-- Summary statistics
WITH concept_status AS (
    SELECT 
        CASE 
            WHEN c.concept_id IS NULL THEN 'MISSING'
            WHEN c.invalid_reason IS NOT NULL THEN 'INVALID'
            WHEN c.standard_concept != 'S' THEN 'NON_STANDARD'
            ELSE 'VALID'
        END as status,
        COUNT(*) as count
    FROM (
        -- All concept IDs from our configuration
        SELECT DISTINCT concept_id FROM (
            VALUES 
            (8507), (8532), (45880669), (45878463),
            (3011536), (3026070), (3024261), (4260747), (40758410),
            (3002549), (43054890), (46234792), (4266763), (40485048),
            (3048230), (3002209), (3012266), (3036844), (3001105), (3050433),
            (1175623), (3024973), (3036322), (3038318), (3038608),
            (4059478), (4128833), (40490322), (40760182), (40760183), (42537958),
            (3002314), (3043737), (4058439), (4072438), (4089559), (44817092),
            (4092289), (4013886), (4012732), (4079978), (4013503), (435559),
            (4067106), (432303), (4066746), (4128331), (4053936), (4236484),
            (433260), (4098004), (198874), (4081422), (4136974), (40485567)
        ) AS t(concept_id)
    ) all_concepts
    LEFT JOIN concept c ON all_concepts.concept_id = c.concept_id
    GROUP BY 
        CASE 
            WHEN c.concept_id IS NULL THEN 'MISSING'
            WHEN c.invalid_reason IS NOT NULL THEN 'INVALID'
            WHEN c.standard_concept != 'S' THEN 'NON_STANDARD'
            ELSE 'VALID'
        END
)
SELECT 
    'Concept Validation Summary' as check_type,
    status,
    count,
    ROUND(100.0 * count / SUM(count) OVER (), 2) as percentage
FROM concept_status
ORDER BY 
    CASE status 
        WHEN 'VALID' THEN 1
        WHEN 'NON_STANDARD' THEN 2
        WHEN 'INVALID' THEN 3
        WHEN 'MISSING' THEN 4
    END;