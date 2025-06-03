library(testthat)
library(dplyr)
library(ggplot2)
library(tidyr)
library(tibble)
library(patchwork)
library(arrow)

# Source the utility functions to be tested
source("../02_aggregation.R")

# Context for the tests
context("Aggregation utilities")

# Create test data
create_test_data <- function() {
  set.seed(42)  # For reproducibility
  
  # Create dataset with multiple backends, methods, datasets, and run_timestamps
  backends <- c("apptainer", "conda", "envmodules")
  methods <- c("kmeans", "dbscan", "hdbscan")
  datasets <- c("atom", "iris", "tetra")
  generators <- c("fcps", "uci")
  metrics <- c("ari", "ami")
  run_timestamps <- c("202301010000", "202301020000")
  
  # Create test data
  test_data <- tibble::tibble(
    backend = factor(rep(backends, each = 10)),
    method = factor(rep(rep(methods, each = 3), length.out = 30)),
    dataset_name = factor(rep(datasets, length.out = 30)),
    dataset_generator = factor(rep(generators, length.out = 30)),
    metric = factor(rep(metrics, length.out = 30)),
    run_timestamp = factor(rep(run_timestamps, length.out = 30)),
    score = runif(30, 0.5, 1.0),
    execution_time_seconds = runif(30, 0.1, 5.0)
  )
  
  # Create a consistent case (all identical scores)
  consistent_rows <- test_data$dataset_name == "iris" & 
                     test_data$method == "kmeans" & 
                     test_data$run_timestamp == "202301010000"
  test_data$score[consistent_rows] <- 0.75
  
  return(test_data)
}

# Tests
test_that("compare_backend_plots returns a patchwork object", {
  # Create test data
  test_data <- create_test_data()
  
  # Generate comparison plots
  plots <- compare_backend_plots(test_data)
  
  # Check that the result is a patchwork object
  expect_true(inherits(plots, "patchwork"))
  
  # Check that we have some plots
  expect_true(length(plots$patches) > 0)
})

test_that("check_score_consistency correctly identifies consistent scores", {
  # Create test data
  test_data <- create_test_data()
  
  # Add a completely consistent case (all runs have identical scores)
  consistent_data <- test_data %>%
    dplyr::filter(dataset_name == "iris", method == "kmeans", dataset_generator == "fcps", metric == "ari")
  
  consistent_data$score <- consistent_data$score[1]  # Set all scores to the same value
  
  # Replace this case in the test data
  test_data <- test_data %>%
    dplyr::filter(!(dataset_name == "iris" & method == "kmeans" & dataset_generator == "fcps" & metric == "ari")) %>%
    dplyr::bind_rows(consistent_data)
  
  # Check consistency
  consistency <- check_score_consistency(test_data)
  
  # Verify that the consistent case is identified
  consistent_case <- consistency %>%
    dplyr::filter(dataset_name == "iris", method == "kmeans", dataset_generator == "fcps", metric == "ari")
  
  expect_true(consistent_case$is_consistent)
  expect_equal(consistent_case$score_range, 0)
  expect_equal(consistent_case$score_sd, 0)
  
  # Verify there are inconsistent cases too
  inconsistent_cases <- consistency %>% dplyr::filter(!is_consistent)
  expect_gt(nrow(inconsistent_cases), 0)
})

test_that("summarize_backend_consistency returns the correct structure", {
  # Create test data
  test_data <- create_test_data()
  
  # Generate summary
  backend_summary <- summarize_backend_consistency(test_data)
  
  # Check structure
  expect_true("backend" %in% names(backend_summary))
  expect_true("percent_consistent" %in% names(backend_summary))
  expect_true("avg_score_range" %in% names(backend_summary))
  
  # Check we have one row per backend
  expect_equal(nrow(backend_summary), length(unique(test_data$backend)))
})

test_that("compare_backend_plots works with subset of backends", {
  # Create test data
  test_data <- create_test_data()
  
  # Test with a subset of backends
  subset_backends <- c("apptainer", "conda")
  plots <- compare_backend_plots(test_data, backends = subset_backends)
  
  # Check that plots are created for the specified backends
  expect_true(inherits(plots, "patchwork"))
  expect_true(length(plots$patches) > 0)
})

test_that("compare_backend_plots fails with less than 2 backends", {
  # Create test data
  test_data <- create_test_data()
  
  # Should fail with only one backend
  expect_error(compare_backend_plots(test_data, backends = c("apptainer")))
})