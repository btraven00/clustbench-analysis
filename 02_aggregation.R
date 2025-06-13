# Utility functions for aggregation metrics and backend comparisons

library(dplyr)
library(ggplot2)
library(tidyr)
library(tibble)
library(patchwork)

#' Compare execution times and scores between backends
#'
#' @description Creates diagonal scatter plots comparing execution times and scores
#'              between pairs of backends. Data points are first aggregated by
#'              (dataset_generator + dataset_name) x method x metric, calculating
#'              the mean execution time and score across all repetitions. This ensures
#'              a fair comparison of the same workload across different backends.
#'
#' @param data A tibble with benchmark data containing at least: backend, method,
#'             dataset_name, dataset_generator, metric, execution_time_seconds, and score columns
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

  # First aggregate the data by the canonical key (dataset_generator + dataset_name x method x metric) and backend
  # This averages execution times and scores across all repetitions for each unique combination
  aggregated_data <- data %>%
    dplyr::group_by(backend, dataset_generator, dataset_name, method, metric) %>%
    dplyr::summarize(
      avg_execution_time = mean(execution_time_seconds, na.rm = TRUE),
      avg_score = mean(score, na.rm = TRUE),
      .groups = "drop"
    )

  # Generate all pairs of backends
  backend_pairs <- expand.grid(backend1 = backends, backend2 = backends, stringsAsFactors = FALSE) %>%
    dplyr::filter(backend1 < backend2)  # Only keep unique pairs

  # Create time comparison plots
  time_plots <- lapply(1:nrow(backend_pairs), function(i) {
    b1 <- backend_pairs$backend1[i]
    b2 <- backend_pairs$backend2[i]

    # Filter aggregated data for the two backends
    times_b1 <- aggregated_data %>%
      dplyr::filter(backend == b1) %>%
      dplyr::select(method, dataset_name, dataset_generator, metric, time_b1 = avg_execution_time)

    times_b2 <- aggregated_data %>%
      dplyr::filter(backend == b2) %>%
      dplyr::select(method, dataset_name, dataset_generator, metric, time_b2 = avg_execution_time)

    # Join the two datasets on the canonical key
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

    # Filter aggregated data for the two backends
    scores_b1 <- aggregated_data %>%
      dplyr::filter(backend == b1) %>%
      dplyr::select(method, dataset_name, dataset_generator, metric, score_b1 = avg_score)

    scores_b2 <- aggregated_data %>%
      dplyr::filter(backend == b2) %>%
      dplyr::select(method, dataset_name, dataset_generator, metric, score_b2 = avg_score)

    # Join the two datasets on the canonical key
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

#' Check if seeds are present in the data
#'
#' @description Determines if the dataset contains seed information
#'
#' @param data A tibble with benchmark data
#'
#' @return Logical value indicating if seeds are present
#'
#' @examples
#' \dontrun{
#' data <- load_from_parquet("path/to/file.parquet")
#' has_seeds <- has_seed_data(data)
#' }
has_seed_data <- function(data) {
  return("seed" %in% colnames(data) &&
         sum(!is.na(data$seed)) > 0)
}

#' Analyze score variability across seeds
#'
#' @description For each method and dataset combination, calculates the
#'              variability in scores across different seeds
#'
#' @param data A tibble with benchmark data containing a seed column
#'
#' @return A tibble with seed variability metrics
#'
#' @examples
#' \dontrun{
#' data <- load_from_parquet("path/to/file.parquet")
#' if (has_seed_data(data)) {
#'   seed_variability <- analyze_seed_variability(data)
#' }
#' }
analyze_seed_variability <- function(data) {
  if (!has_seed_data(data)) {
    warning("No seed data found in the dataset")
    return(NULL)
  }

  # Group by dataset, method, metric and analyze variability across seeds
  seed_variability <- data %>%
    dplyr::filter(!is.na(seed)) %>%
    dplyr::group_by(dataset_generator, dataset_name, method, metric, backend) %>%
    dplyr::summarize(
      n_seeds = dplyr::n_distinct(seed),
      mean_score = mean(score, na.rm = TRUE),
      min_score = min(score, na.rm = TRUE),
      max_score = max(score, na.rm = TRUE),
      score_range = max_score - min_score,
      score_sd = sd(score, na.rm = TRUE),
      coefficient_of_variation = score_sd / abs(mean_score + 1e-10),  # Avoid division by zero
      .groups = "drop"
    ) %>%
    dplyr::filter(n_seeds > 1) %>%  # Only include combinations with multiple seeds
    dplyr::arrange(desc(coefficient_of_variation))  # Sort by most variable first

  return(seed_variability)
}

#' Plot method scores with seed variability
#'
#' @description Creates a plot showing method performance with error bars
#'              representing the variability across seeds
#'
#' @param data A tibble with benchmark data containing a seed column
#' @param metric_filter Optional string to filter for a specific metric
#' @param dataset_filter Optional string to filter for a specific dataset name
#' @param method_filter Optional character vector to filter for specific methods
#' @param top_n Optional integer to limit to the top N methods by score
#'
#' @return A ggplot object showing method scores with seed variability
#'
#' @examples
#' \dontrun{
#' data <- load_from_parquet("path/to/file.parquet")
#' if (has_seed_data(data)) {
#'   plot_method_seed_variability(data, metric_filter = "adjusted_rand_score")
#' }
#' }
plot_method_seed_variability <- function(data,
                                         metric_filter = NULL,
                                         dataset_filter = NULL,
                                         method_filter = NULL,
                                         top_n = NULL) {
  if (!has_seed_data(data)) {
    warning("No seed data found in the dataset")
    return(ggplot() +
             annotate("text", x = 0.5, y = 0.5, label = "No seed data available") +
             theme_void())
  }

  # Filter data if requested
  filtered_data <- data %>% dplyr::filter(!is.na(seed))

  if (!is.null(metric_filter)) {
    filtered_data <- filtered_data %>% dplyr::filter(metric == metric_filter)
  } else if (dplyr::n_distinct(filtered_data$metric) > 1) {
    # If multiple metrics and no filter, use the first one
    first_metric <- levels(filtered_data$metric)[1]
    filtered_data <- filtered_data %>% dplyr::filter(metric == first_metric)
    warning(paste("Multiple metrics found. Using", first_metric,
                  "for plotting. Specify a metric_filter for a different metric."))
  }

  if (!is.null(dataset_filter)) {
    filtered_data <- filtered_data %>% dplyr::filter(dataset_name == dataset_filter)
  }

  if (!is.null(method_filter)) {
    filtered_data <- filtered_data %>% dplyr::filter(method %in% method_filter)
  }

  # Aggregate scores by method and seed
  method_seed_scores <- filtered_data %>%
    dplyr::group_by(method, seed, dataset_name, backend) %>%
    dplyr::summarize(
      avg_score = mean(score, na.rm = TRUE),
      .groups = "drop"
    )

  # Calculate mean and standard deviation across seeds for each method
  method_stats <- method_seed_scores %>%
    dplyr::group_by(method, dataset_name, backend) %>%
    dplyr::summarize(
      mean_score = mean(avg_score, na.rm = TRUE),
      sd_score = sd(avg_score, na.rm = TRUE),
      n_seeds = dplyr::n_distinct(seed),
      .groups = "drop"
    ) %>%
    dplyr::filter(n_seeds > 1)  # Only include methods with multiple seeds

  # Optionally limit to top N methods by score
  if (!is.null(top_n) && is.numeric(top_n)) {
    top_methods <- method_stats %>%
      dplyr::arrange(desc(mean_score)) %>%
      dplyr::slice_head(n = top_n) %>%
      dplyr::pull(method)

    method_stats <- method_stats %>%
      dplyr::filter(method %in% top_methods)
  }

  # Create the plot
  p <- ggplot(method_stats, aes(x = method, y = mean_score, fill = backend)) +
    geom_bar(stat = "identity", position = position_dodge(width = 0.9), alpha = 0.7) +
    geom_errorbar(
      aes(ymin = mean_score - sd_score, ymax = mean_score + sd_score),
      position = position_dodge(width = 0.9),
      width = 0.25
    ) +
    labs(
      title = paste("Method Performance with Seed Variability",
                   ifelse(!is.null(metric_filter), paste0(" (", metric_filter, ")"), "")),
      subtitle = paste("Error bars show standard deviation across",
                      ifelse(is.null(dataset_filter), "all datasets", dataset_filter)),
      x = "Method",
      y = "Average Score",
      caption = "Analysis of seed variability in clustering methods"
    ) +
    theme_minimal() +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "top"
    )

  if (dplyr::n_distinct(method_stats$dataset_name) > 1) {
    p <- p + facet_wrap(~dataset_name, scales = "free_y")
  }

  return(p)
}

#' Compute coefficient of variation for method-dataset combinations
#'
#' @description Calculates the coefficient of variation (CV = sd/mean) for each
#'              method-dataset-metric combination across different seeds
#'
#' @param data A tibble with benchmark data containing a seed column
#'
#' @return A tibble with CV values for each combination
#'
#' @examples
#' \dontrun{
#' data <- load_from_parquet("path/to/file.parquet")
#' if (has_seed_data(data)) {
#'   cv_data <- compute_seed_cv(data)
#' }
#' }
compute_seed_cv <- function(data) {
  if (!has_seed_data(data)) {
    warning("No seed data found in the dataset")
    return(NULL)
  }

  # Calculate CV across seeds
  cv_by_combination <- data %>%
    dplyr::filter(!is.na(seed)) %>%
    dplyr::group_by(dataset_generator, dataset_name, method, metric, backend) %>%
    dplyr::summarize(
      n_seeds = dplyr::n_distinct(seed),
      mean_score = mean(score, na.rm = TRUE),
      sd_score = sd(score, na.rm = TRUE),
      cv = sd_score / abs(mean_score + 1e-10),  # Add small constant to avoid division by zero
      .groups = "drop"
    ) %>%
    dplyr::filter(n_seeds > 1) %>%  # Only keep combinations with multiple seeds
    dplyr::arrange(desc(cv))

  return(cv_by_combination)
}

#' Plot heatmap of seed sensitivity
#'
#' @description Creates a heatmap showing the sensitivity of methods to seeds
#'              across different datasets
#'
#' @param data A tibble with benchmark data containing a seed column
#' @param metric_filter Optional string to filter for a specific metric
#' @param max_methods Optional integer to limit the number of methods shown
#' @param max_datasets Optional integer to limit the number of datasets shown
#'
#' @return A ggplot object showing the heatmap of seed sensitivity
#'
#' @examples
#' \dontrun{
#' data <- load_from_parquet("path/to/file.parquet")
#' if (has_seed_data(data)) {
#'   plot_seed_sensitivity_heatmap(data, metric_filter = "adjusted_rand_score")
#' }
#' }
plot_seed_sensitivity_heatmap <- function(data,
                                         metric_filter = NULL,
                                         max_methods = NULL,
                                         max_datasets = NULL) {
  if (!has_seed_data(data)) {
    warning("No seed data found in the dataset")
    return(ggplot() +
             annotate("text", x = 0.5, y = 0.5, label = "No seed data available") +
             theme_void())
  }

  # Filter data if requested
  filtered_data <- data %>% dplyr::filter(!is.na(seed))

  if (!is.null(metric_filter)) {
    filtered_data <- filtered_data %>% dplyr::filter(metric == metric_filter)
  } else if (dplyr::n_distinct(filtered_data$metric) > 1) {
    # If multiple metrics and no filter, use the first one
    first_metric <- levels(filtered_data$metric)[1]
    filtered_data <- filtered_data %>% dplyr::filter(metric == first_metric)
    warning(paste("Multiple metrics found. Using", first_metric,
                  "for plotting. Specify a metric_filter for a different metric."))
  }

  # Calculate CV
  cv_data <- compute_seed_cv(filtered_data)

  if (is.null(cv_data) || nrow(cv_data) == 0) {
    warning("No valid data for heatmap")
    return(ggplot() +
             annotate("text", x = 0.5, y = 0.5, label = "No valid data for heatmap") +
             theme_void())
  }

  # Optionally limit the number of methods and datasets
  if (!is.null(max_methods) && is.numeric(max_methods)) {
    top_methods <- cv_data %>%
      dplyr::group_by(method) %>%
      dplyr::summarize(avg_cv = mean(cv, na.rm = TRUE)) %>%
      dplyr::arrange(desc(avg_cv)) %>%
      dplyr::slice_head(n = max_methods) %>%
      dplyr::pull(method)

    cv_data <- cv_data %>% dplyr::filter(method %in% top_methods)
  }

  if (!is.null(max_datasets) && is.numeric(max_datasets)) {
    top_datasets <- cv_data %>%
      dplyr::group_by(dataset_name) %>%
      dplyr::summarize(avg_cv = mean(cv, na.rm = TRUE)) %>%
      dplyr::arrange(desc(avg_cv)) %>%
      dplyr::slice_head(n = max_datasets) %>%
      dplyr::pull(dataset_name)

    cv_data <- cv_data %>% dplyr::filter(dataset_name %in% top_datasets)
  }

  # Create the heatmap
  p <- ggplot(cv_data, aes(x = method, y = dataset_name, fill = cv)) +
    geom_tile() +
    scale_fill_viridis_c(name = "Coefficient of\nVariation (CV)") +
    labs(
      title = paste("Seed Sensitivity Heatmap",
                   ifelse(!is.null(metric_filter), paste0(" (", metric_filter, ")"), "")),
      subtitle = "Higher CV indicates greater sensitivity to random seed values",
      x = "Method",
      y = "Dataset",
      caption = "Color indicates coefficient of variation (sd/mean) across seeds"
    ) +
    theme_minimal() +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "right"
    )

  if (dplyr::n_distinct(cv_data$backend) > 1) {
    p <- p + facet_wrap(~backend)
  }

  return(p)
}
