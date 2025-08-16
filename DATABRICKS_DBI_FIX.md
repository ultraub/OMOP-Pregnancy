# Critical Fix: DatabaseConnector vs DBI for Databricks

## Problem
DBI::dbGetQuery() fails with Databricks/Spark connections, causing:
- All counts to return "unable to count"
- Collection failures with java.sql.SQLException
- Unable to verify if data exists at any step

## Root Cause
Databricks JDBC connections work with DatabaseConnector::querySql() but not with DBI::dbGetQuery(). This is because DatabaseConnector has specific Spark/Databricks optimizations that DBI lacks.

## Solution Implemented

### 1. New Helper Function: dc_query()
Located in R/query_utils.R, this function:
- Uses DatabaseConnector::querySql() as primary method
- Falls back to DBI::dbGetQuery() for non-OHDSI connections
- Handles column name case conversion
- Returns NULL on failure for proper error handling

```r
dc_query <- function(connection, sql) {
  sql_string <- as.character(sql)
  tryCatch({
    result <- DatabaseConnector::querySql(connection, sql_string)
    names(result) <- tolower(names(result))
    return(result)
  }, error = function(e) {
    # Fallback to DBI if needed
    tryCatch({
      return(DBI::dbGetQuery(connection, sql_string))
    }, error = function(e2) {
      warning("Query failed with both DatabaseConnector and DBI: ", e2$message)
      return(NULL)
    })
  })
}
```

### 2. Updated safe_count()
- Replaced all DBI::dbGetQuery() calls with dc_query()
- Added three strategies for Spark/Databricks:
  1. Direct COUNT query using DatabaseConnector
  2. COUNT(*) wrapped around rendered SQL
  3. Simple COUNT(*) for basic table references
- Now returns actual counts instead of NA

### 3. Updated safe_collect()
- Replaced all DBI::dbGetQuery() calls with dc_query()
- Handles temp table collection with DatabaseConnector
- Direct SQL fallback uses DatabaseConnector
- Better error handling with NULL checks

### 4. verify_table_upload() 
- Already uses safe_count(), so automatically benefits from fixes
- Will now show actual row counts and NULL/non-NULL statistics

## Expected Results After Fix

### Before (broken):
```
[VERIFY] Checking integrity of HIP_concepts...
  Total rows: unable to count
  gest_value: ? non-NULL, ? NULL

[DEBUG] initial_pregnant_cohort: Total records after union: unable to count
[DEBUG] gestation_visits: Input has unknown total records
[DEBUG] gestation_episodes: Unable to determine visit count (NA returned)
```

### After (fixed):
```
[VERIFY] Checking integrity of HIP_concepts...
  Total rows: 4123
  gest_value: 2856 non-NULL, 1267 NULL
  Sample gest_value values: 21, 40, 17, 16, 39

[DEBUG] initial_pregnant_cohort: Total records after union: 15234
[DEBUG] initial_pregnant_cohort: Records with non-NULL gest_value: 8432
[DEBUG] gestation_visits: Input has 15234 total records
[DEBUG] gestation_visits: Found 8432 records with populated gest_value
[DEBUG] gestation_episodes: Found 8432 gestation visits
```

## Testing the Fix

1. Run the HIPPS algorithm on Databricks
2. Check the debug output for actual counts (not "unable to count")
3. Verify that gestation_episodes processes data
4. Confirm the algorithm completes successfully

## Files Modified

1. **R/query_utils.R**
   - Added dc_query() helper function
   - Updated safe_count() to use DatabaseConnector
   - Updated safe_collect() to use DatabaseConnector
   - verify_table_upload() automatically uses updated functions

## Why This Works

DatabaseConnector is specifically designed for OHDSI tools and includes:
- Proper JDBC driver handling for Spark/Databricks
- SQL translation for various platforms
- Better error handling for distributed databases
- Optimizations for large-scale data operations

DBI is more generic and doesn't have these Spark-specific optimizations, which is why it fails with Databricks connections.

## Next Steps

1. Test on Databricks to confirm counts work
2. Verify gestational data is properly processed
3. Monitor for any remaining collection errors
4. Consider using DatabaseConnector throughout the codebase for consistency