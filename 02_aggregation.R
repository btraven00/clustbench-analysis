# Utility functions for aggregation metrics and backend comparisons

library(dplyr)
library(ggplot2)
library(tidyr)
library(tibble)
library(patchwork)

#' Compare execution times and scores between backends
#'
#' @description Creates diagonal scatter plots comparing execution times and scores
#'              between pairs of backends
#'
#' @param data A tibble with benchmark data containing at least: backend, method, 
#'             dataset_name, execution_time_seconds, and score columns
#' @param backends Optional character vector of backends to compare (default: all backends in data)
#'
#' @return A patchwork object containing the comparison plots
#'
#' @examples
#' \dontrun{
#' data <- load_from_parquet("path/to/file.parquet")
#' compare_backend_plots(data)
#' }
compare_backend_plots <- function(data, backends = NULL) {
  if (is.null(backends)) {
    backends <- levels(data$backend)
  }
  
  # Check we have at least two backends to compare
  if (length(backends) < 2) {
    stop("At least two backends are needed for comparison plots")
  }
  
  # Generate all pairs of backends
  backend_pairs <- expand.grid(backend1 = backends, backend2 = backends, stringsAsFactors = FALSE) %>%
    dplyr::filter(backend1 < backend2)  # Only keep unique pairs
  
  # Create time comparison plots
  time_plots <- lapply(1:nrow(backend_pairs), function(i) {
    b1 <- backend_pairs$backend1[i]
    b2 <- backend_pairs$backend2[i]
    
    # Filter data for the two backends and join them
    times_b1 <- data %>% 
      dplyr::filter(backend == b1) %>%
      dplyr::select(method, dataset_name, dataset_generator, metric, time_b1 = execution_time_seconds)
    
    times_b2 <- data %>% 
      dplyr::filter(backend == b2) %>%
      dplyr::select(method, dataset_name, dataset_generator, metric, time_b2 = execution_time_seconds)
    
    # Join the two datasets
    times_compare <- times_b1 %>%
      dplyr::inner_join(times_b2, by = c("method", "dataset_name", "dataset_generator", "metric"))
    
    # Create plot
    p <- ggplot(times_compare, aes(x = time_b1, y = time_b2)) +
      geom_point(alpha = 0.5) +
      geom_abline(color = "red", linetype = "dashed") +
      scale_x_log10() +
      scale_y_log10() +
      labs(
        title = paste("Execution Time:", b1, "vs", b2),
        x = paste(b1, "Time (seconds)"),
        y = paste(b2, "Time (seconds)")
      ) +
      theme_minimal()
    
    return(p)
  })
  
  # Create score comparison plots
  score_plots <- lapply(1:nrow(backend_pairs), function(i) {
    b1 <- backend_pairs$backend1[i]
    b2 <- backend_pairs$backend2[i]
    
    # Filter data for the two backends and join them
    scores_b1 <- data %>% 
      dplyr::filter(backend == b1) %>%
      dplyr::select(method, dataset_name, dataset_generator, metric, score_b1 = score)
    
    scores_b2 <- data %>% 
      dplyr::filter(backend == b2) %>%
      dplyr::select(method, dataset_name, dataset_generator, metric, score_b2 = score)
    
    # Join the two datasets
    scores_compare <- scores_b1 %>%
      dplyr::inner_join(scores_b2, by = c("method", "dataset_name", "dataset_generator", "metric"))
    
    # Create plot
    p <- ggplot(scores_compare, aes(x = score_b1, y = score_b2)) +
      geom_point(alpha = 0.5) +
      geom_abline(color = "red", linetype = "dashed") +
      labs(
        title = paste("Score:", b1, "vs", b2),
        x = paste(b1, "Score"),
        y = paste(b2, "Score")
      ) +
      theme_minimal()
    
    return(p)
  })
  
  # Combine plots with patchwork
  all_plots <- c(time_plots, score_plots)
  combined_plot <- wrap_plots(all_plots, ncol = 2)
  
  return(combined_plot)
}

#' Check consistency of scores across repetitions
#'
#' @description Checks if every repetition for each dataset x method x metric 
#'              combination gets exactly the same score
#'
#' @param data A tibble with benchmark data containing at least: dataset_name, 
#'             dataset_generator, method, metric, score, and run_timestamp columns
#'
#' @return A tibble with consistency metrics for each unique combination
#'
#' @examples
#' \dontrun{
#' data <- load_from_parquet("path/to/file.parquet")
#' check_score_consistency(data)
#' }
check_score_consistency <- function(data) {
  # Group by dataset, method, metric and calculate consistency metrics
  consistency_metrics <- data %>%
    dplyr::group_by(dataset_generator, dataset_name, method, metric) %>%
    dplyr::summarize(
      n_repetitions = dplyr::n_distinct(run_timestamp),
      mean_score = mean(score, na.rm = TRUE),
      min_score = min(score, na.rm = TRUE),
      max_score = max(score, na.rm = TRUE),
      score_range = max_score - min_score,
      score_sd = sd(score, na.rm = TRUE),
      is_consistent = (score_range == 0),  # TRUE if all scores are identical
      .groups = "drop"
    ) %>%
    dplyr::arrange(desc(score_range))  # Sort by most inconsistent first
  
  return(consistency_metrics)
}

#' Summarize backend consistency
#'
#' @description Summarizes the consistency of scores across backends
#'
#' @param data A tibble with benchmark data containing at least: backend, dataset_name,
#'             dataset_generator, method, metric, and score columns
#'
#' @return A summary tibble with consistency metrics grouped by backend
#'
#' @examples
#' \dontrun{
#' data <- load_from_parquet("path/to/file.parquet")
#' summarize_backend_consistency(data)
#' }
summarize_backend_consistency <- function(data) {
  # Get consistency metrics
  consistency <- check_score_consistency(data)
  
  # Join back with original data to include backend
  backend_consistency <- data %>%
    dplyr::select(backend, dataset_generator, dataset_name, method, metric) %>%
    dplyr::distinct() %>%
    dplyr::inner_join(consistency, by = c("dataset_generator", "dataset_name", "method", "metric"))
  
  # Create summary by backend
  backend_summary <- backend_consistency %>%
    dplyr::group_by(backend) %>%
    dplyr::summarize(
      total_combinations = dplyr::n(),
      consistent_combinations = sum(is_consistent),
      percent_consistent = round(100 * consistent_combinations / total_combinations, 2),
      avg_score_range = mean(score_range, na.rm = TRUE),
      max_score_range = max(score_range, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(desc(percent_consistent))
  
  return(backend_summary)
}