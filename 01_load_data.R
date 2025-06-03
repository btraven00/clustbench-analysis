library(arrow)
library(dplyr)

#' Find the most recent Parquet file in a directory
#'
#' @description Searches for Parquet files in a directory and returns the path to the most recent one
#'
#' @param dir Character string with the directory path to search (default: ".")
#' @param pattern Regular expression pattern to match filenames (default: "*.parquet")
#'
#' @return Character string with the path to the most recent Parquet file
#'
#' @examples
#' \dontrun{
#' path <- find_parquet_file("data")
#' data <- load_from_parquet(path)
#' }
find_parquet_file <- function(dir = ".", pattern = "*.parquet") {
  # List all parquet files in the directory
  files <- list.files(dir, pattern = pattern, full.names = TRUE)
  
  if (length(files) == 0) {
    stop(paste("No Parquet files found in", dir))
  }
  
  # Get file info including modification time
  file_info <- file.info(files)
  
  # Find the most recent file
  most_recent <- rownames(file_info)[which.max(file_info$mtime)]
  
  message(paste("Using most recent Parquet file:", most_recent))
  return(most_recent)
}

#' Load data from Parquet file
#'
#' @description Reads a Parquet file and converts categorical columns to factors
#'
#' @param path Character string with the path to the Parquet file
#'
#' @return A tibble with the following columns as factors:
#'   - backend: Factor with backend system names
#'   - dataset_generator: Factor with dataset generator names
#'   - dataset_name: Factor with dataset names
#'   - method: Factor with clustering algorithm names
#'   - metric: Factor with evaluation metric names
#'
#' @examples
#' \dontrun{
#' data <- load_from_parquet("path/to/file.parquet")
#' }
load_from_parquet <- function(path) {
  d <- tibble(read_parquet(path)) %>%
    mutate(
      backend = factor(backend),
      dataset_generator = factor(dataset_generator),
      dataset_name = factor(dataset_name),
      method = factor(method),
      metric = factor(metric)
    )
  return(d)
}
