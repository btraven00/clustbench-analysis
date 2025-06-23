# Utility functions for aggregation metrics and backend comparisons

library(dplyr)
library(ggplot2)
library(tidyr)
library(tibble)
library(patchwork)

#' Compare execution times between backends
#'
#' @description Creates scatter plots comparing execution times between pairs of backends.
#'              Data points are aggregated by (dataset_generator + dataset_name) x method,
#'              calculating the mean execution time across all seeds/repetitions.
#'
#' @param method_data A tibble with method performance data containing at least: backend, method,
#'                    dataset_name, dataset_generator, execution_time_seconds columns
#' @param backends Optional character vector of backends to compare (default: all backends in data)
#'
#' @return A patchwork object containing the comparison plots
#'
#' @examples
#' \dontrun{
#' data <- load_performance_data(".")
#' compare_backend_execution_times(data$method)
#' }
compare_backend_execution_times <- function(method_data, backends = NULL) {
  if (is.null(backends)) {
    backends <- levels(method_data$backend)
  }

  # Check we have at least two backends to compare
  if (length(backends) < 2) {
    stop("At least two backends are needed for comparison plots")
  }

  # Aggregate execution times by dataset x method x backend
  aggregated_data <- method_data %>%
    dplyr::group_by(backend, dataset_generator, dataset_name, method) %>%
    dplyr::summarize(
      avg_execution_time = mean(execution_time_seconds, na.rm = TRUE),
      .groups = "drop"
    )

  # Generate all pairs of backends
  backend_pairs <- expand.grid(backend1 = backends, backend2 = backends, stringsAsFactors = FALSE) %>%
    dplyr::filter(backend1 < backend2)  # Only keep unique pairs

  # Create execution time comparison plots
  time_plots <- list()
  for (i in 1:nrow(backend_pairs)) {
    b1 <- backend_pairs$backend1[i]
    b2 <- backend_pairs$backend2[i]

    times_b1 <- aggregated_data %>%
      dplyr::filter(backend == b1) %>%
      dplyr::select(dataset_generator, dataset_name, method, avg_execution_time)

    times_b2 <- aggregated_data %>%
      dplyr::filter(backend == b2) %>%
      dplyr::select(dataset_generator, dataset_name, method, avg_execution_time)

    times_compare <- dplyr::inner_join(times_b1, times_b2,
                                      by = c("dataset_generator", "dataset_name", "method"),
                                      suffix = c("_b1", "_b2"))

    p <- ggplot(times_compare, aes(x = avg_execution_time_b1, y = avg_execution_time_b2)) +
      geom_point(alpha = 0.6) +
      geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "red") +
      scale_x_log10() + scale_y_log10() +
      labs(
        title = paste("Execution Time:", b1, "vs", b2),
        x = paste("Execution Time (s) -", b1),
        y = paste("Execution Time (s) -", b2)
      ) +
      theme_minimal()

    time_plots[[paste(b1, b2, sep = "_vs_")]] <- p
  }

  combined_plot <- wrap_plots(time_plots, ncol = 2)
  return(combined_plot)
}

#' Compare clustering scores between backends
#'
#' @description Creates scatter plots comparing clustering scores between pairs of backends.
#'              Data points are aggregated by (dataset_generator + dataset_name) x method x metric,
#'              calculating the mean score across all seeds/repetitions.
#'
#' @param metric_data A tibble with metric performance data containing at least: backend, method,
#'                    dataset_name, dataset_generator, metric, score columns
#' @param backends Optional character vector of backends to compare (default: all backends in data)
#'
#' @return A patchwork object containing the comparison plots
#'
#' @examples
#' \dontrun{
#' data <- load_performance_data(".")
#' compare_backend_scores(data$metric)
#' }
compare_backend_scores <- function(metric_data, backends = NULL) {
  if (is.null(backends)) {
    backends <- levels(metric_data$backend)
  }

  # Check we have at least two backends to compare
  if (length(backends) < 2) {
    stop("At least two backends are needed for comparison plots")
  }

  # Aggregate scores by dataset x method x metric x backend
  aggregated_data <- metric_data %>%
    dplyr::group_by(backend, dataset_generator, dataset_name, method, metric) %>%
    dplyr::summarize(
      avg_score = mean(score, na.rm = TRUE),
      .groups = "drop"
    )

  # Generate all pairs of backends
  backend_pairs <- expand.grid(backend1 = backends, backend2 = backends, stringsAsFactors = FALSE) %>%
    dplyr::filter(backend1 < backend2)  # Only keep unique pairs

  # Create score comparison plots
  score_plots <- list()
  for (i in 1:nrow(backend_pairs)) {
    b1 <- backend_pairs$backend1[i]
    b2 <- backend_pairs$backend2[i]

    scores_b1 <- aggregated_data %>%
      dplyr::filter(backend == b1) %>%
      dplyr::select(dataset_generator, dataset_name, method, metric, avg_score)

    scores_b2 <- aggregated_data %>%
      dplyr::filter(backend == b2) %>%
      dplyr::select(dataset_generator, dataset_name, method, metric, avg_score)

    scores_compare <- dplyr::inner_join(scores_b1, scores_b2,
                                       by = c("dataset_generator", "dataset_name", "method", "metric"),
                                       suffix = c("_b1", "_b2"))

    p <- ggplot(scores_compare, aes(x = avg_score_b1, y = avg_score_b2)) +
      geom_point(alpha = 0.6) +
      geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "red") +
      labs(
        title = paste("Clustering Scores:", b1, "vs", b2),
        x = paste("Score -", b1),
        y = paste("Score -", b2)
      ) +
      theme_minimal()

    score_plots[[paste(b1, b2, sep = "_vs_")]] <- p
  }

  combined_plot <- wrap_plots(score_plots, ncol = 2)
  return(combined_plot)
}

#' Compare both execution times and scores between backends
#'
#' @description Creates combined plots comparing both execution times and clustering scores
#'              between pairs of backends.
#'
#' @param method_data A tibble with method performance data
#' @param metric_data A tibble with metric performance data
#' @param backends Optional character vector of backends to compare (default: all backends in data)
#'
#' @return A patchwork object containing both time and score comparison plots
#'
#' @examples
#' \dontrun{
#' data <- load_performance_data(".")
#' compare_backend_plots(data$method, data$metric)
#' }
compare_backend_plots <- function(method_data, metric_data, backends = NULL) {
  time_plots <- compare_backend_execution_times(method_data, backends)
  score_plots <- compare_backend_scores(metric_data, backends)

  combined_plot <- time_plots / score_plots
  return(combined_plot)
}

#' Check consistency of scores across repetitions
#'
#' @description Checks if every repetition for each dataset x method x metric
#'              combination gets exactly the same score
#'
#' @param metric_data A tibble with metric performance data containing at least: dataset_name,
#'                    dataset_generator, method, metric, score, and run_timestamp columns
#'
#' @return A tibble with consistency metrics for each unique combination
#'
#' @examples
#' \dontrun{
#' data <- load_performance_data(".")
#' check_score_consistency(data$metric)
#' }
check_score_consistency <- function(metric_data) {
  # Group by dataset, method, metric and calculate consistency metrics
  consistency_metrics <- metric_data %>%
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
#' @param metric_data A tibble with metric performance data containing at least: backend, dataset_name,
#'                    dataset_generator, method, metric, and score columns
#'
#' @return A summary tibble with consistency metrics grouped by backend
#'
#' @examples
#' \dontrun{
#' data <- load_performance_data(".")
#' summarize_backend_consistency(data$metric)
#' }
summarize_backend_consistency <- function(metric_data) {
  # Get consistency metrics
  consistency <- check_score_consistency(metric_data)

  # Join back with original data to include backend
  backend_consistency <- metric_data %>%
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

#' Check if seeds are present in the data
#'
#' @description Helper function to check if seed data is available for variability analysis
#'
#' @param data A tibble (either method or metric data)
#'
#' @return Logical indicating whether seed data is present and varies
#'
#' @examples
#' \dontrun{
#' data <- load_performance_data(".")
#' has_seed_data(data$method)
#' }
has_seed_data <- function(data) {
  "seed" %in% names(data) && length(unique(data$seed[!is.na(data$seed)])) > 1
}

#' Analyze seed variability in execution times
#'
#' @description Analyzes how much execution times vary across different seeds
#'              for the same dataset x method combination
#'
#' @param method_data A tibble with method performance data that includes seed column
#'
#' @return A tibble with variability metrics for each dataset x method combination
#'
#' @examples
#' \dontrun{
#' data <- load_performance_data(".")
#' if (has_seed_data(data$method)) {
#'   analyze_execution_time_variability(data$method)
#' }
#' }
analyze_execution_time_variability <- function(method_data) {
  if (!has_seed_data(method_data)) {
    stop("Seed data is required for variability analysis")
  }

  # Calculate variability metrics for execution times
  time_variability <- method_data %>%
    dplyr::filter(!is.na(seed)) %>%
    dplyr::group_by(dataset_generator, dataset_name, method) %>%
    dplyr::summarize(
      n_seeds = dplyr::n(),
      mean_execution_time = mean(execution_time_seconds, na.rm = TRUE),
      median_execution_time = median(execution_time_seconds, na.rm = TRUE),
      sd_execution_time = sd(execution_time_seconds, na.rm = TRUE),
      cv_execution_time = sd_execution_time / mean_execution_time,
      min_execution_time = min(execution_time_seconds, na.rm = TRUE),
      max_execution_time = max(execution_time_seconds, na.rm = TRUE),
      range_execution_time = max_execution_time - min_execution_time,
      .groups = "drop"
    ) %>%
    dplyr::arrange(desc(cv_execution_time))

  return(time_variability)
}

#' Analyze seed variability in clustering scores
#'
#' @description Analyzes how much clustering scores vary across different seeds
#'              for the same dataset x method x metric combination
#'
#' @param metric_data A tibble with metric performance data that includes seed column
#'
#' @return A tibble with variability metrics for each dataset x method x metric combination
#'
#' @examples
#' \dontrun{
#' data <- load_performance_data(".")
#' if (has_seed_data(data$metric)) {
#'   analyze_score_variability(data$metric)
#' }
#' }
analyze_score_variability <- function(metric_data) {
  if (!has_seed_data(metric_data)) {
    stop("Seed data is required for variability analysis")
  }

  # Calculate variability metrics for scores
  score_variability <- metric_data %>%
    dplyr::filter(!is.na(seed)) %>%
    dplyr::group_by(dataset_generator, dataset_name, method, metric) %>%
    dplyr::summarize(
      n_seeds = dplyr::n(),
      mean_score = mean(score, na.rm = TRUE),
      median_score = median(score, na.rm = TRUE),
      sd_score = sd(score, na.rm = TRUE),
      cv_score = sd_score / abs(mean_score),  # Use abs to handle negative scores
      min_score = min(score, na.rm = TRUE),
      max_score = max(score, na.rm = TRUE),
      range_score = max_score - min_score,
      .groups = "drop"
    ) %>%
    dplyr::arrange(desc(cv_score))

  return(score_variability)
}

#' Plot method seed variability for scores
#'
#' @description Creates a plot showing how clustering scores vary across seeds
#'              for different methods on a specific dataset and metric
#'
#' @param metric_data A tibble with metric performance data that includes seed column
#' @param dataset_gen Character string specifying the dataset generator
#' @param dataset_nm Character string specifying the dataset name
#' @param metric_name Character string specifying the metric to plot
#' @param top_n Integer specifying how many top methods to show (default: 10)
#'
#' @return A ggplot object showing seed variability
#'
#' @examples
#' \dontrun{
#' data <- load_performance_data(".")
#' plot_method_seed_variability(data$metric, "fcps", "atom", "adjusted_rand_score")
#' }
plot_method_seed_variability <- function(metric_data, dataset_gen, dataset_nm, metric_name, top_n = 10) {
  if (!has_seed_data(metric_data)) {
    stop("Seed data is required for seed variability plots")
  }

  # Filter data for the specific dataset and metric
  filtered_data <- metric_data %>%
    dplyr::filter(dataset_generator == dataset_gen,
                  dataset_name == dataset_nm,
                  metric == metric_name,
                  !is.na(seed))

  if (nrow(filtered_data) == 0) {
    stop("No data found for the specified dataset and metric combination")
  }

  # Calculate method statistics and select top methods by mean score
  method_stats <- filtered_data %>%
    dplyr::group_by(method) %>%
    dplyr::summarize(
      mean_score = mean(score, na.rm = TRUE),
      n_seeds = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::filter(n_seeds > 1) %>%  # Only methods with multiple seeds
    dplyr::arrange(desc(mean_score)) %>%
    dplyr::slice_head(n = top_n)

  # Filter data to top methods
  filtered_data <- filtered_data %>%
    dplyr::filter(method %in% method_stats$method) %>%
    dplyr::mutate(method = factor(method, levels = method_stats$method))

  # Create the plot
  p <- ggplot(filtered_data, aes(x = method, y = score)) +
    geom_boxplot(aes(fill = method), alpha = 0.7) +
    geom_jitter(width = 0.2, alpha = 0.6) +
    labs(
      title = paste("Seed Variability for", metric_name),
      subtitle = paste("Dataset:", dataset_gen, "-", dataset_nm, "(Top", top_n, "methods)"),
      x = "Method",
      y = "Score"
    ) +
    theme_minimal() +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "none"
    )

  return(p)
}

#' Compute coefficient of variation for seeds
#'
#' @description Computes coefficient of variation (CV) for different combinations
#'              to identify which have the most variable results across seeds
#'
#' @param metric_data A tibble with metric performance data that includes seed column
#' @param group_vars Character vector specifying grouping variables (default: all key variables)
#'
#' @return A tibble with CV values for each combination
#'
#' @examples
#' \dontrun{
#' data <- load_performance_data(".")
#' compute_seed_cv(data$metric)
#' }
compute_seed_cv <- function(metric_data, group_vars = c("dataset_generator", "dataset_name", "method", "metric")) {
  if (!has_seed_data(metric_data)) {
    stop("Seed data is required for CV computation")
  }

  # Compute CV by the specified grouping variables
  cv_by_combination <- metric_data %>%
    dplyr::filter(!is.na(seed)) %>%
    dplyr::group_by(across(all_of(group_vars))) %>%
    dplyr::summarize(
      n_seeds = dplyr::n(),
      mean_score = mean(score, na.rm = TRUE),
      sd_score = sd(score, na.rm = TRUE),
      cv_score = sd_score / abs(mean_score),
      .groups = "drop"
    ) %>%
    dplyr::filter(n_seeds > 1) %>%  # Only combinations with multiple seeds
    dplyr::arrange(desc(cv_score))

  return(cv_by_combination)
}

#' Plot seed sensitivity heatmap
#'
#' @description Creates a heatmap showing coefficient of variation across
#'              dataset x method combinations for a specific metric
#'
#' @param metric_data A tibble with metric performance data that includes seed column
#' @param metric_name Character string specifying the metric to analyze
#' @param top_methods Integer specifying how many top methods to show (default: 15)
#' @param top_datasets Integer specifying how many top datasets to show (default: 10)
#'
#' @return A ggplot object showing the heatmap
#'
#' @examples
#' \dontrun{
#' data <- load_performance_data(".")
#' plot_seed_sensitivity_heatmap(data$metric, "adjusted_rand_score")
#' }
plot_seed_sensitivity_heatmap <- function(metric_data, metric_name, top_methods = 15, top_datasets = 10) {
  if (!has_seed_data(metric_data)) {
    stop("Seed data is required for seed sensitivity heatmap")
  }

  # Filter for the specific metric
  filtered_data <- metric_data %>%
    dplyr::filter(metric == metric_name, !is.na(seed))

  if (nrow(filtered_data) == 0) {
    stop(paste("No data found for metric:", metric_name))
  }

  # Compute CV data
  cv_data <- compute_seed_cv(filtered_data, c("dataset_generator", "dataset_name", "method"))

  # Select top methods by average CV
  top_methods_list <- cv_data %>%
    dplyr::group_by(method) %>%
    dplyr::summarize(avg_cv = mean(cv_score, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(desc(avg_cv)) %>%
    dplyr::slice_head(n = top_methods) %>%
    dplyr::pull(method)

  # Select top datasets by average CV
  top_datasets_list <- cv_data %>%
    dplyr::group_by(dataset_generator, dataset_name) %>%
    dplyr::summarize(avg_cv = mean(cv_score, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(desc(avg_cv)) %>%
    dplyr::slice_head(n = top_datasets)

  # Filter CV data to top methods and datasets
  cv_data <- cv_data %>%
    dplyr::filter(method %in% top_methods_list) %>%
    dplyr::semi_join(top_datasets_list, by = c("dataset_generator", "dataset_name")) %>%
    dplyr::mutate(dataset = paste(dataset_generator, dataset_name, sep = "_"))

  # Create the heatmap
  p <- ggplot(cv_data, aes(x = method, y = dataset, fill = cv_score)) +
    geom_tile() +
    scale_fill_viridis_c(name = "CV") +
    labs(
      title = paste("Seed Sensitivity Heatmap -", metric_name),
      subtitle = paste("Top", top_methods, "methods and", top_datasets, "datasets by CV"),
      x = "Method",
      y = "Dataset"
    ) +
    theme_minimal() +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      axis.text.y = element_text(size = 8)
    )

  return(p)
}
