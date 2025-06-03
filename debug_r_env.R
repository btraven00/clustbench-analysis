# Debug R Environment
# This script helps diagnose issues with R package loading in Nix environments

cat("=== R Environment Debugging ===\n\n")

# System information
cat("R version:\n")
print(R.version)
cat("\n")

cat("R home directory:\n")
print(R.home())
cat("\n")

# Library paths
cat("Library paths (.libPaths()):\n")
print(.libPaths())
cat("\n")

# Environment variables
cat("R_LIBS environment variable:\n")
print(Sys.getenv("R_LIBS"))
cat("\n")

cat("R_LIBS_USER environment variable:\n")
print(Sys.getenv("R_LIBS_USER"))
cat("\n")

cat("R_LIBS_SITE environment variable:\n")
print(Sys.getenv("R_LIBS_SITE"))
cat("\n")

cat("R_HOME environment variable:\n")
print(Sys.getenv("R_HOME"))
cat("\n")

# List installed packages
cat("Installed packages:\n")
tryCatch({
  installed_pkgs <- installed.packages()[, "Package"]
  print(sort(installed_pkgs))
}, error = function(e) {
  cat("Error listing installed packages:", e$message, "\n")
})
cat("\n")

# Test loading common packages
test_package <- function(pkg_name) {
  result <- tryCatch({
    require(pkg_name, character.only = TRUE)
  }, error = function(e) {
    return(paste("ERROR:", e$message))
  }, warning = function(w) {
    return(paste("WARNING:", w$message))
  })
  
  if (isTRUE(result)) {
    return("SUCCESS")
  } else if (is.character(result)) {
    return(result)
  } else {
    return("FAILED (unknown reason)")
  }
}

packages_to_test <- c("knitr", "rmarkdown", "tidyverse", "ggplot2", "arrow", 
                     "shiny", "flexdashboard", "DT", "patchwork", 
                     "kableExtra", "plotly", "scales", "testthat")

cat("Package loading tests:\n")
for (pkg in packages_to_test) {
  cat(sprintf("%-15s: %s\n", pkg, test_package(pkg)))
}
cat("\n")

# Check if packages exist in the file system
check_pkg_in_paths <- function(pkg_name, lib_paths) {
  found <- FALSE
  locations <- character(0)
  
  for (path in lib_paths) {
    pkg_path <- file.path(path, pkg_name)
    if (dir.exists(pkg_path)) {
      found <- TRUE
      locations <- c(locations, pkg_path)
    }
  }
  
  if (found) {
    return(list(found = TRUE, locations = locations))
  } else {
    return(list(found = FALSE, locations = character(0)))
  }
}

cat("Physical package location check:\n")
for (pkg in packages_to_test) {
  result <- check_pkg_in_paths(pkg, .libPaths())
  if (result$found) {
    cat(sprintf("%-15s: FOUND at %s\n", pkg, paste(result$locations, collapse = ", ")))
  } else {
    cat(sprintf("%-15s: NOT FOUND in library paths\n", pkg))
  }
}
cat("\n")

# Suggestions
cat("=== Suggestions ===\n")
cat("1. Verify that the shell.nix file includes all required packages\n")
cat("2. Try installing a package manually to see if R can write to the library paths:\n")
cat("   install.packages('knitr')\n")
cat("3. Check if R is using the correct library paths from Nix\n")
cat("4. Verify that the .Rprofile file is being loaded\n")
cat("5. Try running R directly from the command line with:\n")
cat("   nix-shell --run \"R\"\n")

cat("\nEnd of diagnostics\n")