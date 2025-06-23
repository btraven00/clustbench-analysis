library(arrow)
library(dplyr)

#' Find the most recent parquet files for methods and metrics
#'
#' @description Searches for method-performance and metric-performance parquet files
#'              and returns paths to the most recent ones
#'
#' @param dir Character string with the directory path to search (default: ".")
#'
#' @return Named list with paths to the most recent method and metric parquet files
#'
#' @examples
#' \dontrun{
#' paths <- find_parquet_files("data")
#' method_data <- load_method_performance(paths$method)
#' metric_data <- load_metric_performance(paths$metric)
#' }
find_parquet_files <- function(dir = ".") {
  # Find method performance files
  method_files <- list.files(dir, pattern = "method-performance.*\\.parquet$", full.names = TRUE)

  # Find metric performance files
  metric_files <- list.files(dir, pattern = "metric-performance.*\\.parquet$", full.names = TRUE)

  if (length(method_files) == 0) {
    stop(paste("No method-performance parquet files found in", dir))
  }

  if (length(metric_files) == 0) {
    stop(paste("No metric-performance parquet files found in", dir))
  }

  # Get the most recent files based on modification time
  method_info <- file.info(method_files)
  most_recent_method <- rownames(method_info)[which.max(method_info$mtime)]

  metric_info <- file.info(metric_files)
  most_recent_metric <- rownames(metric_info)[which.max(metric_info$mtime)]

  message(paste("Using method performance file:", most_recent_method))
  message(paste("Using metric performance file:", most_recent_metric))

  return(list(
    method = most_recent_method,
    metric = most_recent_metric
  ))
}

#' Load method performance data from parquet file
#'
#' @description Reads a method-performance parquet file and converts categorical columns to factors
#'
#' @param path Character string with the path to the method-performance parquet file
#'
#' @return A tibble with method-level performance data including:
#'   - source_dir, backend, run_timestamp: Run metadata
#'   - dataset_generator, dataset_name, true_k, has_noise: Dataset info
#'   - method, seed: Method configuration
#'   - execution_time_seconds, runtime, threads, disk_read, disk_write, avg_load, peak_rss: Performance metrics
#'
#' @examples
#' \dontrun{
#' method_data <- load_method_performance("method-performance_file.parquet")
#' }
load_method_performance <- function(path) {
  d <- tibble(read_parquet(path))

  # Convert categorical columns to factors
  factor_columns <- c("source_dir", "backend", "run_timestamp",
                     "dataset_generator", "dataset_name", "method")

  for (col in factor_columns) {
    if (col %in% names(d)) {
      d[[col]] <- factor(d[[col]])
    }
  }

  # Ensure boolean columns are properly typed
  if ("has_noise" %in% names(d)) {
    d$has_noise <- as.logical(d$has_noise)
  }

  return(d)
}

#' Load metric performance data from parquet file
#'
#' @description Reads a metric-performance parquet file and converts categorical columns to factors
#'
#' @param path Character string with the path to the metric-performance parquet file
#'
#' @return A tibble with metric-level performance data including:
#'   - source_dir, backend, run_timestamp: Run metadata
#'   - dataset_generator, dataset_name, true_k, has_noise: Dataset info
#'   - method, seed, metric: Method and metric configuration
#'   - score, runtime: Metric-specific performance
#'   - duplicate_k_anomaly, empty_file, missing_true_k_score: Quality flags
#'   - k=2, k=3, etc.: Scores for different k values
#'
#' @examples
#' \dontrun{
#' metric_data <- load_metric_performance("metric-performance_file.parquet")
#' }
load_metric_performance <- function(path) {
  d <- tibble(read_parquet(path))

  # Convert categorical columns to factors
  factor_columns <- c("source_dir", "backend", "run_timestamp",
                     "dataset_generator", "dataset_name", "method", "metric")

  for (col in factor_columns) {
    if (col %in% names(d)) {
      d[[col]] <- factor(d[[col]])
    }
  }

  # Ensure boolean columns are properly typed
  boolean_columns <- c("has_noise", "duplicate_k_anomaly", "empty_file", "missing_true_k_score")
  for (col in boolean_columns) {
    if (col %in% names(d)) {
      d[[col]] <- as.logical(d[[col]])
    }
  }

  return(d)
}

#' Load both method and metric performance data
#'
#' @description Convenience function to load both method and metric performance data
#'              from the most recent parquet files in a directory
#'
#' @param dir Character string with the directory path to search (default: ".")
#'
#' @return Named list with two tibbles: 'method' and 'metric'
#'
#' @examples
#' \dontrun{
#' data <- load_performance_data(".")
#' method_data <- data$method
#' metric_data <- data$metric
#' }
load_performance_data <- function(dir = ".") {
  paths <- find_parquet_files(dir)

  method_data <- load_method_performance(paths$method)
  metric_data <- load_metric_performance(paths$metric)

  return(list(
    method = method_data,
    metric = metric_data
  ))
}

#' Legacy function for backward compatibility
#'
#' @description Loads metric performance data (the old default behavior)
#'              This function is kept for backward compatibility with existing code
#'
#' @param path Character string with the path to a parquet file, or directory to search
#'
#' @return A tibble with metric performance data
#'
#' @examples
#' \dontrun{
#' data <- load_from_parquet("metric-performance_file.parquet")
#' }
load_from_parquet <- function(path) {
  # If path is a file, load it directly
  if (file.exists(path) && !dir.exists(path)) {
    # Determine if it's a method or metric file based on filename
    if (grepl("method-performance", basename(path))) {
      return(load_method_performance(path))
    } else {
      return(load_metric_performance(path))
    }
  }

  # If path is a directory, find the most recent metric file
  if (dir.exists(path)) {
    paths <- find_parquet_files(path)
    return(load_metric_performance(paths$metric))
  }

  stop(paste("Path does not exist:", path))
}

#' Legacy function to find a single parquet file
#'
#' @description Finds the most recent metric-performance parquet file
#'              This function is kept for backward compatibility
#'
#' @param dir Character string with the directory path to search (default: ".")
#' @param pattern Regular expression pattern (ignored, kept for compatibility)
#'
#' @return Character string with the path to the most recent metric-performance parquet file
#'
#' @examples
#' \dontrun{
#' path <- find_parquet_file(".")
#' data <- load_from_parquet(path)
#' }
find_parquet_file <- function(dir = ".", pattern = "*.parquet") {
  paths <- find_parquet_files(dir)
  return(paths$metric)
}
