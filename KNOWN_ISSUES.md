# Known Issues and Solutions

## Current Issues

### 1. n_distinct() with Multiple Arguments in dbplyr
**Error**: `Error in n_distinct(person_id, visit_date) : unused argument (visit_date)`

**Cause**: 
- dbplyr's SQL translation doesn't support `n_distinct()` with multiple columns
- SQL Server doesn't have a direct equivalent to `COUNT(DISTINCT col1, col2)`

**Solution**:
```r
# Instead of:
n_distinct(person_id, visit_date)

# Use:
n() # after group_by(person_id, visit_date) and summarize
# Or:
count(distinct(paste(person_id, visit_date)))
```

**Fix Required**: Update line 171 in `run_hipps_outcomes_only.R`

### 2. Missing Gestational Age Data
**Issue**: Tufts database has no gestational age measurements

**Impact**:
- Cannot create gestation-based episodes
- Reduced precision in pregnancy start dates
- Must rely on outcome-based detection only

**Workaround**: 
- Use outcome-only pipeline
- Apply conservative term duration estimates
- Document limitations in results

### 3. SQL Server Compatibility Issues (RESOLVED)

**Fixed Issues**:
- ✅ date_diff → DATEDIFF
- ✅ Date arithmetic → DATEADD
- ✅ Reserved word 'index' → 'row_index'
- ✅ Table references with parentheses
- ✅ slice_max/slice_min alternatives

## Database-Specific Considerations

### SQL Server
- Temp tables use `#` prefix
- Date functions require specific syntax
- No direct COUNT(DISTINCT multiple columns)
- Requires `copy = TRUE` for local dataframe joins

### PostgreSQL
- Temp tables in pg_temp schema
- More flexible date arithmetic
- Supports advanced window functions
- Better NULL handling in aggregates

### Oracle
- Temp tables in TEMP schema
- DATE vs TIMESTAMP considerations
- DUAL table requirements
- Different string concatenation (||)

## Performance Issues

### Large Cohort Processing
**Problem**: Timeout or memory issues with >1M patients

**Solutions**:
1. Process by year/batch
2. Increase timeout settings
3. Use compute() more aggressively
4. Add database indexes on person_id, dates

### Complex Joins
**Problem**: Multi-table joins can be slow

**Solutions**:
1. Create temp tables for intermediate results
2. Filter early in the pipeline
3. Use appropriate join types
4. Ensure database statistics are updated

## Data Quality Issues

### Concept Mapping
**Problem**: Local concepts not in standard OMOP vocabulary

**Solutions**:
1. Use concept_relationship table for mapping
2. Implement fuzzy matching on concept names
3. Create site-specific concept mappings
4. Document unmapped concepts

### Duplicate Episodes
**Problem**: Same pregnancy detected multiple times

**Causes**:
- Multiple data sources
- Transfer between facilities
- Billing duplicates

**Solutions**:
1. Implement episode merging logic
2. Use conservative date windows
3. Prioritize specific outcomes
4. Document merge decisions

## Algorithm Limitations

### HIP Algorithm
- Requires outcome or gestational age codes
- May miss early pregnancy losses
- Limited by coding completeness
- Sensitive to date accuracy

### PPS Algorithm
- Requires regular prenatal care
- Less effective for emergency presentations
- Depends on visit pattern recognition
- May overdetect in high-risk monitoring

### Merge Algorithm
- Conservative overlap windows
- May split single pregnancies
- May merge twin pregnancies
- Requires manual validation for edge cases

## Testing Limitations

### Tufts Test Database
- No gestational age data
- Limited pregnancy outcomes
- Synthetic data characteristics
- Not representative of all populations

### Validation Challenges
- No gold standard for comparison
- Chart review not feasible
- Birth registry linkage unavailable
- Limited demographic data

## Future Improvements

### Planned Enhancements
1. Support for multiple births
2. Pregnancy complication detection
3. Maternal outcome tracking
4. Father/partner linkage
5. Neonatal outcome integration

### Algorithm Refinements
1. Machine learning for episode detection
2. Probabilistic record linkage
3. Natural language processing for notes
4. Temporal pattern mining
5. Cross-site validation framework

## Troubleshooting Guide

### Installation Issues
```bash
# Java configuration for DatabaseConnector
export JAVA_HOME=/path/to/java
export DATABASECONNECTOR_JAR_FOLDER=inst/jdbc

# R package dependencies
install.packages(c("DatabaseConnector", "SqlRender", "dplyr", "dbplyr"))
```

### Connection Issues
```r
# Test connection
con <- connect(connectionDetails)
querySql(con, "SELECT 1 as test")

# Check permissions
querySql(con, "SELECT * FROM INFORMATION_SCHEMA.TABLES")
```

### Query Debugging
```r
# View generated SQL
show_query(lazy_query)

# Test query components
sql <- translate(render(sql_template))
```

### Memory Management
```r
# Clear intermediate results
rm(large_object)
gc()

# Use batching
for(batch in batches) {
  process_batch(batch)
  gc()
}
```

## Support Resources

### Documentation
- Technical Documentation: TECHNICAL_DOCUMENTATION.md
- Algorithm Flowchart: ALGORITHM_FLOWCHART.md
- Architecture Guide: docs/ARCHITECTURE.md

### Community
- OHDSI Forums: https://forums.ohdsi.org
- GitHub Issues: [Package repository]/issues
- Email Support: [Contact information]

### References
- HIPPS Paper: Jones et al. 2023
- All of Us Implementation: Smith et al. 2024
- OMOP CDM Documentation: https://ohdsi.github.io/CommonDataModel/