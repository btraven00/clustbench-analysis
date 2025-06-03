#!/usr/bin/env Rscript
#
# run_tests.R - Main test runner for clustbench-analysis
#
# This script runs all tests in the project using the testthat package.
# Assumes it's being run from the git root folder.
#

# Check if testthat is installed
if (!require("testthat", quietly = TRUE)) {
  cat("Error: The 'testthat' package is not available.\n")
  cat("In a Nix environment, packages cannot be installed automatically.\n")
  cat("Make sure 'testthat' is included in your shell.nix file.\n")
  quit(status = 1)
}

# Print some information
cat("Running tests for clustbench-analysis\n")
cat("Working directory:", getwd(), "\n")

# Run all tests in the tests directory
test_dir("tests")

# Provide a summary
cat("\nTest run complete.\n")