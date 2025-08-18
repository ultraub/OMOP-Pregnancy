#' Assign Confidence Scores to Episodes
#'
#' Assigns confidence scores based on algorithm source and data quality
#'
#' @param episodes Data frame of pregnancy episodes
#' @return Episodes with confidence_score column added
#' @export
assign_confidence_scores <- function(episodes) {
  
  if (nrow(episodes) == 0) {
    return(episodes)
  }
  
  episodes <- episodes %>%
    mutate(
      confidence_score = case_when(
        # High confidence: HIP episodes with specific outcomes
        algorithm_used == "HIP" & outcome_category %in% c("LB", "SB", "DELIV") ~ "High",
        
        # High confidence: Merged episodes (validated by both algorithms)
        algorithm_used == "MERGED" ~ "High",
        
        # High confidence: ESD with week-level precision
        !is.na(precision_category) & precision_category %in% c("week", "two-week") ~ "High",
        
        # Medium confidence: HIP episodes with other outcomes
        algorithm_used == "HIP" & outcome_category %in% c("AB", "SA", "ECT") ~ "Medium",
        
        # Medium confidence: PPS with specific outcomes
        algorithm_used == "PPS" & outcome_category %in% c("LB", "SB", "AB", "SA") ~ "Medium",
        
        # Medium confidence: Month-level precision
        !is.na(precision_category) & precision_category %in% c("month", "two-month", "three-month") ~ "Medium",
        
        # Low confidence: PPS pregnancy-only episodes
        algorithm_used == "PPS" & outcome_category == "PREG" ~ "Low",
        
        # Low confidence: Non-specific dates
        !is.na(precision_category) & precision_category == "non-specific" ~ "Low",
        
        # Default to Low
        TRUE ~ "Low"
      )
    )
  
  return(episodes)
}