# Algorithm Flowchart and Logic Documentation

## HIP Algorithm Detailed Flow

### Stage 1: Initial Cohort Creation

```
START
  │
  ├─> Load HIP_concepts.xlsx
  │     └─> Contains ~500 pregnancy-related concept IDs
  │           Categories: PREG, GEST, LB, SB, AB, SA, ECT, DELIV
  │
  ├─> Query person table
  │     └─> Filter: gender_concept_id = 8532 (Female)
  │
  └─> For each domain table (parallel):
        │
        ├─> procedure_occurrence
        │     WHERE procedure_concept_id IN (HIP_concepts)
        │     AND age_at_procedure BETWEEN 15 AND 55
        │
        ├─> measurement  
        │     WHERE measurement_concept_id IN (HIP_concepts)
        │     AND age_at_measurement BETWEEN 15 AND 55
        │
        ├─> observation
        │     WHERE observation_concept_id IN (HIP_concepts)
        │     AND age_at_observation BETWEEN 15 AND 55
        │
        └─> condition_occurrence
              WHERE condition_concept_id IN (HIP_concepts)
              AND age_at_condition BETWEEN 15 AND 55
                │
                └─> UNION ALL results -> initial_pregnant_cohort
```

### Stage 2: Outcome Visit Processing

```
For each outcome type (AB, SA, DELIV, ECT, LB, SB):
  │
  ├─> Filter initial_cohort by outcome category
  │
  ├─> Load Matcho_outcome_limits for minimum days
  │     AB: 70 days, DELIV: 168 days, ECT: 42 days
  │     LB: 168 days, SA: 70 days, SB: 168 days
  │
  └─> Apply episode detection algorithm:
        │
        ├─> Sort by person_id, visit_date
        │
        ├─> For each person:
        │     │
        │     ├─> First visit = NEW EPISODE
        │     │
        │     └─> Subsequent visits:
        │           │
        │           ├─> Calculate days_since_last = current_date - previous_date
        │           │
        │           ├─> IF days_since_last >= minimum_days:
        │           │     └─> NEW EPISODE
        │           │
        │           └─> ELSE:
        │                 └─> SAME EPISODE (ignore)
        │
        └─> Output: Distinct episodes for outcome type
```

### Stage 3: Hierarchical Combination

```
Hierarchy Order (highest to lowest priority):
1. Stillbirth (SB)
2. Ectopic (ECT)  
3. Abortion (AB/SA)
4. Delivery (DELIV)
5. Livebirth (LB)

Process:
  │
  ├─> Start with LB episodes
  │
  ├─> add_stillbirth():
  │     For each SB episode:
  │       └─> Check for LB within ±1 day
  │             ├─> IF found: Override LB with SB
  │             └─> ELSE: Add SB as new episode
  │
  ├─> add_ectopic():
  │     For each ECT episode:
  │       └─> Check for conflicts within 42 days
  │             ├─> IF conflict: Keep ECT (higher priority)
  │             └─> ELSE: Add ECT as new episode
  │
  ├─> add_abortion():
  │     For each AB/SA episode:
  │       └─> Check for conflicts within 70 days
  │             ├─> IF conflict with higher priority: Skip
  │             └─> ELSE: Add AB/SA as new episode
  │
  └─> add_delivery():
        For each DELIV without specific outcome:
          └─> Check for conflicts within 168 days
                ├─> IF no conflict: Add DELIV
                └─> ELSE: Skip
```

### Stage 4: Gestational Age Processing

```
gestation_visits():
  │
  ├─> Query measurements for gestational age concepts:
  │     - 3048230: Gestational age in weeks
  │     - 3002209: Gestational age in days
  │     - 3012266: Gestational age at birth
  │
  └─> For each measurement:
        │
        ├─> Extract value_as_number (gestational weeks/days)
        │
        ├─> Validate: 0 <= value <= 44 weeks
        │
        └─> Calculate pregnancy_start_date:
              └─> start = measurement_date - (weeks * 7 + days)

gestation_episodes():
  │
  ├─> Group gestation visits by person_id
  │
  ├─> For each person:
  │     │
  │     ├─> Sort by calculated start_date
  │     │
  │     └─> Group into episodes:
  │           │
  │           ├─> IF start_dates within 14 days: SAME EPISODE
  │           │
  │           └─> ELSE: NEW EPISODE
  │
  └─> Calculate episode boundaries:
        ├─> min_start_date: earliest calculated start
        ├─> max_start_date: latest calculated start
        └─> confidence: 1 / (max - min + 1)
```

### Stage 5: Start Date Calculation

```
calculate_start():
  │
  ├─> Load Matcho_term_durations:
  │     Category | Min_weeks | Max_weeks | Min_days | Max_days
  │     ---------|-----------|-----------|----------|----------
  │     LB       | 20        | 44        | 140      | 308
  │     SB       | 20        | 44        | 140      | 308
  │     AB       | 4         | 20        | 28       | 140
  │     SA       | 4         | 20        | 28       | 140
  │     ECT      | 4         | 12        | 28       | 84
  │     DELIV    | 20        | 44        | 140      | 308
  │
  └─> For each outcome episode:
        │
        ├─> Get outcome_date and category
        │
        ├─> Calculate date range:
        │     min_start = outcome_date - max_days
        │     max_start = outcome_date - min_days
        │
        └─> IF gestational age available:
              ├─> Use gestational age-derived start
              └─> ELSE: Use min_start (conservative)
```

## PPS Algorithm Detailed Flow

### Concept Collection

```
input_GT_concepts():
  │
  ├─> Load PPS_concepts.xlsx:
  │     - Contains gestational timing (GT) concepts
  │     - Maps concepts to gestational week ranges
  │     - Example: "First trimester visit" -> weeks 0-12
  │
  └─> Query all 5 OMOP tables:
        │
        ├─> condition_occurrence
        ├─> procedure_occurrence  
        ├─> observation
        ├─> measurement
        └─> visit_occurrence
              │
              └─> WHERE concept_id IN (PPS_concepts)
                    AND person has HIP pregnancy evidence
```

### Episode Creation

```
get_PPS_episodes():
  │
  ├─> For each GT concept found:
  │     │
  │     ├─> Extract gestational_week from PPS_concepts mapping
  │     │
  │     └─> Calculate implied_pregnancy_start:
  │           start = concept_date - (gestational_week * 7)
  │
  └─> Group by person_id and pregnancy:
        │
        ├─> Cluster concepts with consistent timing:
        │     │
        │     ├─> Expected_date = start_date + (concept_week * 7)
        │     │
        │     ├─> IF |actual_date - expected_date| < 14 days:
        │     │     └─> SAME PREGNANCY
        │     │
        │     └─> ELSE:
        │           └─> DIFFERENT PREGNANCY
        │
        └─> Validate episode coherence:
              │
              ├─> Minimum 2 concepts required
              ├─> Concepts must span >= 28 days
              └─> Timing must be internally consistent
```

## Merge Algorithm Logic

### Episode Matching

```
final_merged_episodes():
  │
  ├─> Load HIP episodes and PPS episodes
  │
  ├─> For each person_id:
  │     │
  │     └─> For each HIP episode:
  │           │
  │           ├─> Find overlapping PPS episodes:
  │           │     overlap = episodes where:
  │           │       - PPS.start <= HIP.end + 45 days
  │           │       - PPS.end >= HIP.start - 45 days
  │           │
  │           ├─> Merge overlapping episodes:
  │           │     │
  │           │     ├─> merged.start = MIN(HIP.start, PPS.start)
  │           │     ├─> merged.end = MAX(HIP.end, PPS.end)
  │           │     ├─> merged.category = HIP.category (preferred)
  │           │     └─> merged.confidence = (HIP + PPS) / 2
  │           │
  │           └─> IF no overlap:
  │                 └─> Keep episodes separate
  │
  └─> Remove duplicates:
        │
        ├─> Exact duplicates: Keep one
        ├─> Subset episodes: Keep larger
        └─> Conflicting: Keep higher confidence
```

## ESD Algorithm Refinement

### Timing Information Integration

```
episodes_with_gestational_timing_info():
  │
  ├─> For each merged episode:
  │     │
  │     ├─> Collect ALL timing concepts within episode window:
  │     │     - Gestational age measurements
  │     │     - Prenatal visit codes
  │     │     - Trimester-specific diagnoses
  │     │     - Ultrasound procedures
  │     │
  │     ├─> For each timing concept:
  │     │     │
  │     │     ├─> Calculate implied start date
  │     │     ├─> Assign confidence weight
  │     │     └─> Add to evidence list
  │     │
  │     └─> Statistical consensus:
  │           │
  │           ├─> Remove outliers (>2 SD from mean)
  │           ├─> Calculate weighted average
  │           ├─> Compute confidence interval
  │           └─> Set final start date
  │
  └─> Validate against known constraints:
        │
        ├─> Pregnancy cannot exceed 44 weeks
        ├─> Start must be before outcome
        ├─> Start must be after previous pregnancy + 28 days
        └─> Adjust if constraints violated
```

## Decision Points and Business Logic

### Key Business Rules

1. **Age Limits**: Only women 15-55 years at time of pregnancy
2. **Episode Spacing**: Minimum days between episodes by outcome type
3. **Priority Hierarchy**: Specific outcomes override general ones
4. **Gestational Limits**: 0-44 weeks considered valid
5. **Confidence Thresholds**: Episodes need minimum evidence

### Error Handling

```
At each stage:
  │
  ├─> Check for empty results
  │     └─> Return empty dataframe with correct schema
  │
  ├─> Validate data types
  │     └─> Coerce or error with informative message
  │
  ├─> Handle missing values
  │     └─> Use defaults or skip record
  │
  └─> Catch database errors
        └─> Log error and attempt retry or graceful degradation
```

## Performance Optimization Points

1. **Lazy Evaluation**: Build query chain, execute once
2. **Temp Tables**: Store intermediate results database-side
3. **Indexing**: Rely on database indexes for person_id, dates
4. **Batch Processing**: Process in chunks for large cohorts
5. **Parallel Queries**: Run domain queries simultaneously
6. **Early Filtering**: Apply age/gender filters early
7. **Smart Joins**: Use appropriate join types (inner/left)

## Output Structure

```
Final Episode Record:
{
  person_id: integer,
  episode_id: integer,
  pregnancy_start_date: date,
  pregnancy_end_date: date,
  outcome_date: date,
  outcome_category: string (LB|SB|AB|SA|ECT|DELIV|PREG),
  confidence_score: float (0-1),
  algorithm_source: string (HIP|PPS|MERGED),
  gestational_age_at_outcome: integer (weeks),
  episode_length: integer (days),
  supporting_concepts: integer (count),
  first_evidence_date: date,
  last_evidence_date: date
}
```