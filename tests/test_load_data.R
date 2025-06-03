library(testthat)
library(arrow)
library(dplyr)

# Source the data loading functions
source("../01_load_data.R")

# Context for the tests
context("Data loading functionality")

# Create a temporary parquet file for testing
create_test_parquet <- function() {
  temp_file <- tempfile(fileext = ".parquet")
  
  # Create test data
  test_data <- tibble(
    backend = c("apptainer", "conda", "envmodules"),
    dataset_generator = c("fcps", "uci", "fcps"),
    dataset_name = c("atom", "iris", "tetra"),
    method = c("kmeans", "dbscan", "hdbscan"),
    metric = c("ari", "ami", "nmi"),
    score = c(0.85, 0.92, 0.78),
    execution_time_seconds = c(1.2, 0.8, 2.5)
  )
  
  # Write to parquet
  write_parquet(test_data, temp_file)
  
  return(temp_file)
}

# Tests
test_that("load_from_parquet correctly loads data and converts factors", {
  # Create test file
  test_file <- create_test_parquet()
  
  # Load the test data
  result <- load_from_parquet(test_file)
  
  # Test that result is a tibble
  expect_s3_class(result, "tbl_df")
  
  # Test that specified columns are factors
  expect_s3_class(result$backend, "factor")
  expect_s3_class(result$dataset_generator, "factor")
  expect_s3_class(result$dataset_name, "factor")
  expect_s3_class(result$method, "factor")
  expect_s3_class(result$metric, "factor")
  
  # Test that numeric columns remain numeric
  expect_type(result$score, "double")
  expect_type(result$execution_time_seconds, "double")
  
  # Test factor levels
  expect_setequal(levels(result$backend), c("apptainer", "conda", "envmodules"))
  expect_setequal(levels(result$dataset_generator), c("fcps", "uci"))
  expect_setequal(levels(result$dataset_name), c("atom", "iris", "tetra"))
  expect_setequal(levels(result$method), c("kmeans", "dbscan", "hdbscan"))
  expect_setequal(levels(result$metric), c("ari", "ami", "nmi"))
  
  # Test row count
  expect_equal(nrow(result), 3)
  
  # Clean up
  if (file.exists(test_file)) file.remove(test_file)
})

test_that("load_from_parquet handles missing files correctly", {
  # Test with non-existent file
  expect_error(load_from_parquet("nonexistent_file.parquet"))
})

test_that("find_parquet_file finds the most recent file", {
  # Create a temp directory
  temp_dir <- tempdir()
  
  # Create test files with different timestamps
  file1 <- file.path(temp_dir, "test1.parquet")
  file2 <- file.path(temp_dir, "test2.parquet")
  file3 <- file.path(temp_dir, "test3.parquet")
  
  # Create test data
  test_data <- tibble(x = 1:3)
  
  # Write test files with delays to ensure different timestamps
  write_parquet(test_data, file1)
  Sys.sleep(1)
  write_parquet(test_data, file2)
  Sys.sleep(1)
  write_parquet(test_data, file3)
  
  # Test that the function finds the most recent file
  result <- find_parquet_file(temp_dir)
  expect_equal(result, file3)
  
  # Clean up
  file.remove(file1, file2, file3)
})

test_that("find_parquet_file errors on empty directory", {
  # Create a temporary directory
  temp_dir <- file.path(tempdir(), "empty_dir")
  dir.create(temp_dir, showWarnings = FALSE)
  
  # Test that the function throws an error
  expect_error(find_parquet_file(temp_dir))
  
  # Clean up
  unlink(temp_dir, recursive = TRUE)
})
