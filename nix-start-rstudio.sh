#!/usr/bin/env bash
#
# start-rstudio.sh - Helper script to launch RStudio with the correct R environment
#
# This script ensures RStudio can find all the R packages installed via Nix
#

set -e  # Exit on error

# If not in a Nix shell, enter one and re-run this script
if [[ ! -v IN_NIX_SHELL ]]; then
  echo "Entering Nix shell environment..."
  exec nix-shell --run "bash $0"
  exit $?
fi

# Initialize R's library state first to avoid installed.packages() issues
echo "Initializing R environment..."
R --quiet --vanilla -e 'utils::sessionInfo()'

# Test if R packages are working before continuing
echo "Testing R packages..."
R --quiet --vanilla -e '
  pkg <- "knitr"
  if (!requireNamespace(pkg, quietly = TRUE)) {
    cat("ERROR: Package", pkg, "not found!\n")
    cat("Library paths:", paste(.libPaths(), collapse=", "), "\n")
    
    # Try to help the user
    cat("\nTrying to fix library paths...\n")
    
    # Try to find the package in the Nix store
    nix_dirs <- list.files("/nix/store", pattern="r-knitr-", full.names=TRUE)
    lib_paths <- paste0(nix_dirs, "/library")
    lib_paths <- lib_paths[dir.exists(lib_paths)]
    
    if (length(lib_paths) > 0) {
      cat("Found knitr in Nix store! Adding to .libPaths()\n")
      for (path in lib_paths) {
        cat("Adding:", path, "\n")
        .libPaths(c(path, .libPaths()))
      }
      
      # Check if package is now available
      if (requireNamespace(pkg, quietly = TRUE)) {
        cat("SUCCESS: Package", pkg, "is now available!\n")
        # Write the fix to .Rprofile
        profile_content <- paste0(
          "message(\"Adding Nix library paths...\")\n",
          "nix_paths <- c(\n",
          paste0("  \"", lib_paths, "\"", collapse=",\n"),
          "\n)\n",
          ".libPaths(c(nix_paths, .libPaths()))\n"
        )
        
        cat("Writing fix to .Rprofile...\n")
        write(profile_content, file=".Rprofile", append=TRUE)
      } else {
        cat("ERROR: Still cannot load package", pkg, "\n")
        cat("Try running ./debug_r_env.R for more information\n")
        exit(1)
      }
    } else {
      cat("ERROR: Could not find", pkg, "in Nix store\n")
      cat("Try running ./debug_nix_env.sh for more information\n")
      exit(1)
    }
  } else {
    cat("Package", pkg, "loaded successfully\n")
  }
'

# Check if RStudio is available
if ! command -v rstudio &> /dev/null; then
    echo "Error: rstudio command not found"
    echo "Make sure RStudio is properly installed via Nix"
    exit 1
fi

# Get the project directory
PROJECT_DIR=$(dirname "$0")

# Print environment info
echo "Starting RStudio with Nix R environment..."
echo "R library paths:"
echo " - R_LIBS_USER: $R_LIBS_USER"
echo " - R_LIBS_SITE: $R_LIBS_SITE"
echo " - R_HOME: $R_HOME"
echo ""
echo "Packages have been verified. RStudio should work correctly."

# Launch RStudio with the project
if [ -f "${PROJECT_DIR}/clustbench-analysis.Rproj" ]; then
  rstudio "${PROJECT_DIR}/clustbench-analysis.Rproj" &
else
  rstudio &
fi
