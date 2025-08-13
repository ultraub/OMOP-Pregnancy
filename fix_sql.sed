# Fix sql_date_diff calls
s/sql_date_diff("visit_date", "prev_date", "day")/sql("DATEDIFF(day, prev_date, visit_date)")/g
s/sql_date_diff("next_date", "visit_date", "day")/sql("DATEDIFF(day, visit_date, next_date)")/g
s/sql_date_diff("visit_date", "max_gest_start_date", "day")/sql("DATEDIFF(day, max_gest_start_date, visit_date)")/g
s/sql_date_diff("max_gest_start_date", "min_gest_start_date", "day")/sql("DATEDIFF(day, min_gest_start_date, max_gest_start_date)")/g
s/sql_date_diff("visit_date", "max_gest_date", "day")/sql("DATEDIFF(day, max_gest_date, visit_date)")/g
s/sql_date_diff("min_gest_date_2", "min_gest_date", "day")/sql("DATEDIFF(day, min_gest_date, min_gest_date_2)")/g
s/sql_date_diff("max_gest_date", "end_gest_date", "day")/sql("DATEDIFF(day, end_gest_date, max_gest_date)")/g
s/sql_date_diff("max_gest_start_date", "prev_date", "day")/sql("DATEDIFF(day, prev_date, max_gest_start_date)")/g
s/sql_date_diff("max_start_date", "prev_date", "day")/sql("DATEDIFF(day, prev_date, max_start_date)")/g
s/sql_date_diff("estimated_start_date", "prev_date", "day")/sql("DATEDIFF(day, prev_date, estimated_start_date)")/g
s/sql_date_diff("visit_date", "gest_date", "day")/sql("DATEDIFF(day, gest_date, visit_date)")/g

# Fix sql_date_add calls
s/sql_date_add("visit_date", "-CAST(min_term AS INT)", "day")/sql("DATEADD(day, -CAST(min_term AS INT), visit_date)")/g
s/sql_date_add("visit_date", "-CAST(max_term AS INT)", "day")/sql("DATEADD(day, -CAST(max_term AS INT), visit_date)")/g
s/sql_date_add("max_gest_date", "-CAST(max_gest_day AS INT)", "day")/sql("DATEADD(day, -CAST(max_gest_day AS INT), max_gest_date)")/g
s/sql_date_add("min_gest_date", "-CAST(min_gest_day AS INT)", "day")/sql("DATEADD(day, -CAST(min_gest_day AS INT), min_gest_date)")/g

# Fix sql_concat calls
s/sql_concat(person_id, visit_date, connection = connection)/sql("CONCAT(person_id, visit_date)")/g
s/sql_concat(person_id, max_gest_date, connection = connection)/sql("CONCAT(person_id, max_gest_date)")/g