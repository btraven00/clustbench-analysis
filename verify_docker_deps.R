#!/usr/bin/env Rscript
# Simple verification script for Docker image R packages

cat("=== R Package Verification ===\n\n")

# System information
cat("R version:", R.version.string, "\n\n")
cat("Library paths:\n")
print(.libPaths())
cat("\n")

# Essential packages to check
essential_pkgs <- c(
  "knitr", "rmarkdown", "tidyverse", "xfun"
)

# Test knitr and dependencies specifically
cat("Testing knitr and dependencies:\n")
cat("-------------------------------\n")

# Try to load knitr
if (requireNamespace("knitr", quietly = TRUE)) {
  cat("✓ knitr package loaded successfully\n")
  
  # Get knitr dependencies
  tryCatch({
    knitr_deps <- tools::package_dependencies("knitr", recursive = TRUE)
    deps_to_check <- unique(c(unlist(knitr_deps), essential_pkgs))
    
    missing <- character(0)
    for (pkg in deps_to_check) {
      if (!requireNamespace(pkg, quietly = TRUE)) {
        missing <- c(missing, pkg)
      }
    }
    
    if (length(missing) > 0) {
      cat("\n✗ Missing dependencies: ", paste(missing, collapse = ", "), "\n")
    } else {
      cat("\n✓ All knitr dependencies are available\n")
    }
  }, error = function(e) {
    cat("\n✗ Error checking dependencies:", e$message, "\n")
  })
} else {
  cat("✗ knitr package failed to load\n")
}

cat("\n")

# Try loading essential packages
cat("Essential package test results:\n")
cat("-------------------------------\n")

for (pkg in essential_pkgs) {
  result <- tryCatch({
    requireNamespace(pkg, quietly = TRUE)
  }, error = function(e) {
    cat("Error for", pkg, ":", e$message, "\n")
    FALSE
  })
  
  status <- if (result) "✓" else "✗"
  cat(sprintf("%s %s\n", status, pkg))
}

cat("\n=== Summary ===\n")
pkgs <- rownames(installed.packages())
cat("Total packages installed:", length(pkgs), "\n")

# Check the most important packages
pkgs_missing <- character(0)
for (pkg in c("xfun", "knitr", "rmarkdown")) {
  if (!(pkg %in% pkgs)) {
    pkgs_missing <- c(pkgs_missing, pkg)
  }
}

if (length(pkgs_missing) > 0) {
  cat("✗ Missing critical packages:", paste(pkgs_missing, collapse=", "), "\n")
} else {
  cat("✓ All critical packages are installed\n")
}

# Add diagnostic information about file permissions
cat("\nFile permission check:\n")
cat("----------------------\n")
r_lib <- .libPaths()[1]
cat("Main R library path:", r_lib, "\n")
if (file.access(r_lib, mode=2) == 0) {
  cat("✓ R library path is writable\n")
} else {
  cat("✗ R library path is NOT writable\n")
}