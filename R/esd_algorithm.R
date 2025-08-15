#' ESD (Estimated Start Date) Algorithm
#'
#' Functions to calculate estimated pregnancy start dates using gestational
#' timing information. This code maintains maximum similarity to the original 
#' All of Us implementation.
#'
#' @name esd_algorithm
#' @import dplyr
#' @import lubridate
#' @import stringr
#' @import purrr
NULL

#' Get timing concepts for episodes
#'
#' @param concept_tbl Concept table
#' @param condition_occurrence_tbl Condition occurrence table
#' @param observation_tbl Observation table
#' @param measurement_tbl Measurement table
#' @param procedure_occurrence_tbl Procedure occurrence table
#' @param final_merged_episode_detailed_df Final merged episodes
#' @param PPS_concepts PPS concepts table
#' @param config Configuration list (optional)
#'
#' @return Data frame with timing concepts
#' @export
get_timing_concepts <- function(concept_tbl, condition_occurrence_tbl, observation_tbl,
                                measurement_tbl, procedure_occurrence_tbl, 
                                final_merged_episode_detailed_df, PPS_concepts, config = NULL) {
  # obtain the gestational timing <= 3 month concept information to use as additional 
  # information for precision category designation
  
  # Ensure we have a proper dataframe, not a lazy query
  if (inherits(final_merged_episode_detailed_df, c("tbl_lazy", "tbl_sql"))) {
    message("Computing merged episodes dataframe...")
    pregnant_dates <- final_merged_episode_detailed_df %>% collect()
  } else {
    pregnant_dates <- final_merged_episode_detailed_df
  }
  
  # Check if we have any data
  if (is.null(pregnant_dates) || nrow(pregnant_dates) == 0) {
    warning("No episodes found in merged dataframe - returning empty result")
    return(data.frame())
  }
  
  algo2_timing_concepts_id_list <- PPS_concepts %>%
    select(domain_concept_id) %>%
    pull(domain_concept_id) %>%
    as.integer()
  
  # Get concept lists from config or use defaults
  if (is.null(config)) {
    # Use hardcoded defaults if no config provided
    observation_concept_list <- c(3011536, 3026070, 3024261, 4260747, 40758410, 3002549, 
                                  43054890, 46234792, 4266763, 40485048, 3048230, 3002209, 3012266)
    measurement_concept_list <- c(3036844, 3048230, 3001105, 3002209, 3050433, 3012266)
    est_date_of_delivery_concepts <- c(1175623, 1175623, 3001105, 3011536, 3024261, 3024973, 
                                       3026070, 3036322, 3038318, 3038608, 4059478, 4128833, 
                                       40490322, 40760182, 40760183, 42537958)
    est_date_of_conception_concepts <- c(3002314, 3043737, 4058439, 4072438, 4089559, 44817092)
    len_of_gestation_at_birth_concepts <- c(4260747, 43054890, 46234792, 4266763, 40485048)
  } else {
    # Use configured concept IDs
    observation_concept_list <- if (!is.null(config$concepts$gestational_age_concepts$observation_concepts)) {
      config$concepts$gestational_age_concepts$observation_concepts
    } else {
      c(3011536, 3026070, 3024261, 4260747, 40758410, 3002549, 
        43054890, 46234792, 4266763, 40485048, 3048230, 3002209, 3012266)
    }
    
    measurement_concept_list <- if (!is.null(config$concepts$gestational_age_concepts$measurement_concepts)) {
      config$concepts$gestational_age_concepts$measurement_concepts
    } else {
      c(3036844, 3048230, 3001105, 3002209, 3050433, 3012266)
    }
    
    est_date_of_delivery_concepts <- if (!is.null(config$concepts$pregnancy_dating$estimated_delivery_date_concepts)) {
      config$concepts$pregnancy_dating$estimated_delivery_date_concepts
    } else {
      c(1175623, 1175623, 3001105, 3011536, 3024261, 3024973, 
        3026070, 3036322, 3038318, 3038608, 4059478, 4128833, 
        40490322, 40760182, 40760183, 42537958)
    }
    
    est_date_of_conception_concepts <- if (!is.null(config$concepts$pregnancy_dating$estimated_conception_date_concepts)) {
      config$concepts$pregnancy_dating$estimated_conception_date_concepts
    } else {
      c(3002314, 3043737, 4058439, 4072438, 4089559, 44817092)
    }
    
    len_of_gestation_at_birth_concepts <- if (!is.null(config$concepts$pregnancy_dating$length_of_gestation_at_birth_concepts)) {
      config$concepts$pregnancy_dating$length_of_gestation_at_birth_concepts
    } else {
      c(4260747, 43054890, 46234792, 4266763, 40485048)
    }
  }
  
  # need to find concept names that contain 'gestation period' as well as the specific concepts
  # Use SQL LIKE instead of str_detect for database compatibility
  concepts_to_search <- concept_tbl %>%
    filter(
      sql("LOWER(concept_name) LIKE '%gestation period%'") |
        concept_id %in% c(
          observation_concept_list, measurement_concept_list, algo2_timing_concepts_id_list,
          est_date_of_delivery_concepts, est_date_of_conception_concepts,
          len_of_gestation_at_birth_concepts
        )
    ) %>%
    select(concept_id, concept_name)
  
  get_preg_related_concepts <- function(df, person_id_list, df_date_col) {
    df %>%
      select(person_id, all_of(df_date_col), concept_id, concept_name, value_col) %>%
      rename(all_of(c(domain_concept_start_date = df_date_col))) %>%
      inner_join(person_id_list, by = join_by(
        person_id, domain_concept_start_date >= start_date,
        domain_concept_start_date <= recorded_episode_end
      ), suffix = c(".x", ".y")) %>%
      select(person_id, domain_concept_start_date,
        domain_concept_id = concept_id, domain_concept_name = concept_name,
        start_date, recorded_episode_end, value_col, episode_number
      )
  }
  
  # add: change to pregnancy start rather than recorded episode start
  # Handle different column names that might exist
  # Check which columns are available
  col_names <- names(pregnant_dates)
  
  # Debug: print available columns
  message("Available columns in pregnant_dates: ", paste(col_names, collapse = ", "))
  
  # Find date columns (any column with 'date' in the name)
  date_cols <- col_names[grepl("date", col_names, ignore.case = TRUE)]
  
  # Determine start date column
  if ("pregnancy_start" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(start_date = pregnancy_start)
  } else if ("estimated_start_date" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(start_date = estimated_start_date)
  } else if ("episode_min_date" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(start_date = episode_min_date)
  } else if ("merged_episode_start" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(start_date = merged_episode_start)
  } else if ("visit_date" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(start_date = visit_date)
  } else if ("final_outcome_date" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(start_date = final_outcome_date)
  } else if (length(date_cols) > 0) {
    warning(paste("No recognized start date column found - using", date_cols[1]))
    pregnant_dates <- pregnant_dates %>% mutate(start_date = !!sym(date_cols[1]))
  } else {
    # Create a dummy date if no date columns exist
    warning("No date columns found - creating dummy dates")
    pregnant_dates <- pregnant_dates %>% mutate(start_date = as.Date("2020-01-01"))
  }
  
  # Determine end date column
  if ("recorded_episode_end" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(end_date = recorded_episode_end)
  } else if ("pregnancy_end" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(end_date = pregnancy_end)
  } else if ("episode_max_date" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(end_date = episode_max_date)
  } else if ("merged_episode_end" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(end_date = merged_episode_end)
  } else if ("visit_date" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(end_date = visit_date)
  } else if ("final_outcome_date" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(end_date = final_outcome_date)
  } else if (length(date_cols) > 0) {
    # Use the last date column if available
    last_date_col <- date_cols[length(date_cols)]
    warning(paste("No recognized end date column found - using", last_date_col))
    pregnant_dates <- pregnant_dates %>% mutate(end_date = !!sym(last_date_col))
  } else {
    warning("No recognized end date column found - using start_date")
    pregnant_dates <- pregnant_dates %>% mutate(end_date = start_date)
  }
  
  # Determine episode number column
  if ("episode_number" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(episode_num = episode_number)
  } else if ("episode" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(episode_num = episode)
  } else if ("person_episode_number" %in% col_names) {
    pregnant_dates <- pregnant_dates %>% mutate(episode_num = person_episode_number)
  } else {
    pregnant_dates <- pregnant_dates %>% mutate(episode_num = row_number())
  }
  
  # Get the connection from the original lazy query if available
  connection <- NULL
  if (inherits(final_merged_episode_detailed_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- final_merged_episode_detailed_df$src$con
  }
  
  # Prepare the person_id_list dataframe
  person_id_list_df <- pregnant_dates %>%
    select(person_id, start_date, recorded_episode_end = end_date, episode_number = episode_num) %>%
    filter(!is.na(start_date))
  
  # Create temp table with proper parameter order
  if (!is.null(connection)) {
    person_id_list <- create_temp_table(connection, person_id_list_df)
  } else {
    # If no connection available, just use the dataframe as-is
    person_id_list <- person_id_list_df
  }
  
  c_o <- concepts_to_search %>%
    inner_join(condition_occurrence_tbl, by = c("concept_id" = "condition_concept_id"), suffix = c(".x", ".y")) %>%
    mutate(value_col = concept_name) %>%
    get_preg_related_concepts(person_id_list, "condition_start_date")
  o_df <- concepts_to_search %>%
    inner_join(observation_tbl, by = c("concept_id" = "observation_concept_id"), suffix = c(".x", ".y")) %>%
    mutate(value_col = value_as_string) %>%
    get_preg_related_concepts(person_id_list, "observation_date")
  m_df <- concepts_to_search %>%
    inner_join(measurement_tbl, by = c("concept_id" = "measurement_concept_id"), suffix = c(".x", ".y")) %>%
    mutate(value_col = value_as_number) %>%
    get_preg_related_concepts(person_id_list, "measurement_date")
  p_df <- concepts_to_search %>%
    inner_join(procedure_occurrence_tbl, by = c("concept_id" = "procedure_concept_id"), suffix = c(".x", ".y")) %>%
    mutate(value_col = concept_name) %>%
    get_preg_related_concepts(person_id_list, "procedure_date")
  
  preg_related_concepts <- list(c_o, o_df, mutate(m_df, value_col = as.character(value_col)), p_df) %>%
    reduce(union_all)
  
  algo2_timing_concepts_df <- PPS_concepts %>%
    select(domain_concept_id, min_month, max_month)
  
  # Get gestational age concepts from config for validation
  gestational_age_validation_concepts <- if (!is.null(config)) {
    c(config$concepts$gestational_age_concepts$gestational_age_in_weeks,
      config$concepts$gestational_age_concepts$gestational_age_estimated,
      config$concepts$gestational_age_concepts$gestational_age)
  } else {
    c(3048230, 3002209, 3012266)
  }
  
  max_gestational_weeks <- if (!is.null(config) && !is.null(config$algorithm_parameters$max_gestational_weeks)) {
    config$algorithm_parameters$max_gestational_weeks
  } else {
    44
  }
  
  preg_related_concepts_local <- preg_related_concepts %>%
    left_join(algo2_timing_concepts_df, by = "domain_concept_id", suffix = c(".x", ".y")) %>%
    collect() %>%
    mutate(domain_value = str_replace(value_col, "\\|text_result_val:", "")) %>%
    mutate(domain_value = str_replace(domain_value, "\\|mapped_text_result_val:", "")) %>%
    mutate(domain_value = str_replace(domain_value, "Gestation period, ", "")) %>%
    mutate(domain_value = str_replace(domain_value, "gestation period, ", "")) %>%
    mutate(domain_value = str_replace(domain_value, " weeks", "")) %>%
    mutate(domain_value = as.integer(as.numeric(domain_value))) %>%
    mutate(
      keep_value = if_else((str_detect(tolower(domain_concept_name), "gestation period,")) |
        (str_detect(tolower(domain_concept_name), "gestational age")) |
        (domain_concept_id %in% gestational_age_validation_concepts & 
         domain_value < max_gestational_weeks & domain_value > 0), 1, 0),
      extrapolated_preg_start = if_else(keep_value == 1, domain_concept_start_date - (domain_value * 7), NA_Date_)
    )
  
  preg_related_concepts_local
}

#' Validate date format
#'
#' @param date_text Date text to validate
#'
#' @return Logical indicating if date is valid
#' @export
validate <- function(date_text) {
  tryCatch(
    {
      as.Date(date_text, "%Y-%m-%d")
      TRUE
    },
    error = function(e) {
      FALSE
    }
  )
}

#' Find intersection of date intervals
#'
#' @param intervals List of date intervals
#'
#' @return Intersection of intervals
#' @export
findIntersection <- function(intervals) {
  if (length(intervals) == 1) {
    intervals <- matrix(intervals[[1]], ncol = 2)
  } else {
    # Sort intervals
    intervals <- reduce(intervals, rbind)
  }
  intervals <- as.data.frame(intervals) %>%
    mutate(across(everything(), as.Date)) %>%
    arrange(V1)
  
  # First remove outlier ranges via the IQR*1.5 approach. Outlier ranges are determined by the number of overlaps each range has with other ranges.
  overlapCountDict <- rep(0, nrow(intervals))
  for (j in 1:nrow(intervals)) {
    for (m in 1:nrow(intervals)) {
      if (j != m) {
        # First interval
        last <- intervals[j, 2]
        first <- intervals[j, 1]
        # 1st condition - does first date equal either the first or last date
        if ((intervals[m, 1] == last) || (intervals[m, 1] == first)) {
          overlapCountDict[j] <- overlapCountDict[j] + 1
          next
          # 2nd condition - does second date equal either the first or last date
        } else if ((intervals[m, 2] == last) || (intervals[m, 2] == first)) {
          overlapCountDict[j] <- overlapCountDict[j] + 1
          next
          # 3rd condition - does second date fall between the first and last date
        } else if ((intervals[m, 2] < last) && (intervals[m, 2] > first)) {
          overlapCountDict[j] <- overlapCountDict[j] + 1
          next
          # 4th condition - does first date fall between the first and last date
        } else if ((intervals[m, 1] < last) && (intervals[m, 1] > first)) {
          overlapCountDict[j] <- overlapCountDict[j] + 1
          next
        }
      }
    }
  }
  
  intervals$overlapCountDict <- overlapCountDict
  # if there were no overlaps with a given date, overlapCountDict is 0
  # but the other ones will be higher
  
  # Remove outliers using IQR method
  Q1 <- quantile(overlapCountDict, 0.25)
  Q3 <- quantile(overlapCountDict, 0.75)
  IQR <- Q3 - Q1
  lower_bound <- Q1 - 1.5 * IQR
  upper_bound <- Q3 + 1.5 * IQR
  
  intervals_filtered <- intervals %>%
    filter(overlapCountDict >= lower_bound & overlapCountDict <= upper_bound)
  
  if (nrow(intervals_filtered) == 0) {
    return(NULL)
  }
  
  # Find the intersection
  max_start <- max(intervals_filtered$V1)
  min_end <- min(intervals_filtered$V2)
  
  if (max_start <= min_end) {
    return(c(max_start, min_end))
  } else {
    return(NULL)
  }
}

#' Get episodes with gestational timing information
#'
#' @param get_timing_concepts_df Timing concepts data frame
#'
#' @return Episodes with gestational timing
#' @export
episodes_with_gestational_timing_info <- function(get_timing_concepts_df) {
  
  # Process each episode to calculate pregnancy start dates
  episodes <- get_timing_concepts_df %>%
    filter(keep_value == 1) %>%
    group_by(person_id, episode_number) %>%
    summarise(
      # Get range of extrapolated pregnancy start dates
      min_extrapolated_start = min(extrapolated_preg_start, na.rm = TRUE),
      max_extrapolated_start = max(extrapolated_preg_start, na.rm = TRUE),
      n_timing_concepts = n(),
      # Get the most common extrapolated start date
      mode_extrapolated_start = as.Date(names(sort(table(extrapolated_preg_start), 
                                                   decreasing = TRUE)[1])),
      .groups = "drop"
    ) %>%
    mutate(
      # Calculate precision based on range of dates
      date_range = as.numeric(max_extrapolated_start - min_extrapolated_start),
      precision_category = case_when(
        date_range <= 7 ~ "High",
        date_range <= 30 ~ "Medium",
        TRUE ~ "Low"
      ),
      # Use mode as best estimate, or min if mode is NA
      estimated_start_date = coalesce(mode_extrapolated_start, min_extrapolated_start)
    )
  
  return(episodes)
}

#' Merge episodes with metadata
#'
#' @param episodes_with_gestational_timing_info_df Episodes with timing info
#' @param final_merged_episode_detailed_df Final merged episodes
#' @param Matcho_term_durations Term duration limits
#'
#' @return Merged episodes with metadata
#' @export
merged_episodes_with_metadata <- function(episodes_with_gestational_timing_info_df,
                                         final_merged_episode_detailed_df,
                                         Matcho_term_durations) {
  
  # Merge timing information with episode details
  merged <- final_merged_episode_detailed_df %>%
    left_join(episodes_with_gestational_timing_info_df,
              by = c("person_id", "episode_number"), suffix = c(".x", ".y"))
  
  # Add term duration information
  merged_with_terms <- merged %>%
    left_join(Matcho_term_durations, by = c("final_category" = "category"), suffix = c(".x", ".y"))
  
  # Calculate final estimated start dates considering all information
  final_episodes <- merged_with_terms %>%
    mutate(
      # Use gestational timing if available, otherwise use outcome-based calculation
      final_estimated_start = coalesce(
        estimated_start_date,
        pregnancy_start,
        final_outcome_date - days(max_term)
      ),
      # Calculate gestational age at outcome
      gestational_age_at_outcome = case_when(
        !is.na(final_outcome_date) & !is.na(final_estimated_start) ~
          as.numeric(final_outcome_date - final_estimated_start) / 7,
        TRUE ~ NA_real_
      ),
      # Validate gestational age is within expected ranges
      gestational_age_valid = case_when(
        is.na(gestational_age_at_outcome) ~ NA,
        gestational_age_at_outcome >= min_term/7 & 
          gestational_age_at_outcome <= max_term/7 ~ TRUE,
        TRUE ~ FALSE
      ),
      # Add quality indicator
      episode_quality = case_when(
        precision_category == "High" & gestational_age_valid == TRUE ~ "High",
        precision_category == "Medium" | gestational_age_valid == TRUE ~ "Medium",
        TRUE ~ "Low"
      )
    )
  
  return(final_episodes)
}

#' Calculate precision categories for episodes
#'
#' @param episodes_df Episodes data frame
#'
#' @return Episodes with precision categories
#' @export
calculate_precision_categories <- function(episodes_df) {
  episodes_df %>%
    mutate(
      precision_category = case_when(
        # High precision: Multiple consistent timing concepts
        n_timing_concepts >= 3 & date_range <= 7 ~ "High",
        # Medium precision: Some timing information
        n_timing_concepts >= 2 | date_range <= 30 ~ "Medium",
        # Low precision: Limited timing information
        TRUE ~ "Low"
      )
    )
}
