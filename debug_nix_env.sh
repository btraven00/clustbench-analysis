#!/usr/bin/env bash
#
# debug_nix_env.sh - Diagnose issues with Nix and R integration
#

echo "=== Nix and R Environment Diagnostics ==="
echo

# Check if we're in a Nix shell
if [[ -v IN_NIX_SHELL ]]; then
  echo "✓ Running inside a Nix shell"
else
  echo "✗ NOT running inside a Nix shell"
  echo "  Try running: nix-shell"
  echo
fi

# Check Nix and R versions
echo "=== Version Information ==="
echo "Nix version: $(nix --version 2>/dev/null || echo "nix command not found")"
echo "R version: $(R --version | head -n 1 2>/dev/null || echo "R command not found")"
echo "RStudio version: $(rstudio --version 2>/dev/null || echo "rstudio command not found")"
echo

# Check environment variables
echo "=== R Environment Variables ==="
echo "R_HOME: ${R_HOME:-"not set"}"
echo "R_LIBS: ${R_LIBS:-"not set"}"
echo "R_LIBS_USER: ${R_LIBS_USER:-"not set"}"
echo "R_LIBS_SITE: ${R_LIBS_SITE:-"not set"}"
echo

# Check R library paths
echo "=== R Library Paths ==="
if command -v R &>/dev/null; then
  R --quiet --no-save <<EOF
  cat("R library paths (.libPaths()):\n")
  print(.libPaths())
  cat("\n")
EOF
else
  echo "R command not available"
fi
echo

# Check if R packages exist in Nix store
echo "=== Checking R Packages in Nix Store ==="
PACKAGES=("knitr" "rmarkdown" "tidyverse" "ggplot2" "arrow" "shiny" "flexdashboard" "DT" "patchwork" "kableExtra" "plotly" "scales" "testthat")

if [[ -v R_LIBS_USER ]]; then
  for pkg in "${PACKAGES[@]}"; do
    if [[ -d "${R_LIBS_USER}/${pkg}" ]]; then
      echo "✓ ${pkg} found in ${R_LIBS_USER}/${pkg}"
    else
      echo "✗ ${pkg} NOT found in ${R_LIBS_USER}"
    fi
  done
else
  echo "Cannot check packages: R_LIBS_USER not set"
fi
echo

# Check .Rprofile
echo "=== .Rprofile Information ==="
if [[ -f .Rprofile ]]; then
  echo "✓ .Rprofile exists"
  echo "Contents:"
  echo "----------"
  cat .Rprofile
  echo "----------"
else
  echo "✗ .Rprofile not found in current directory"
fi
echo

# Check if Rprofile is actually loaded
echo "=== Testing if .Rprofile is loaded ==="
R --quiet --no-save <<EOF
cat("If you see 'Loading Nix R environment...' below, .Rprofile was loaded:\n")
EOF
echo

# Suggest fixes
echo "=== Suggestions ==="
echo "1. Make sure you're running in a Nix shell: nix-shell"
echo "2. Try running R directly: nix-shell --run R"
echo "3. Check if shell.nix has the correct R packages"
echo "4. Try running R with custom library paths:"
echo "   R_LIBS_USER=${R_LIBS_USER} R_LIBS_SITE=${R_LIBS_SITE} R"
echo "5. Try setting .libPaths() manually in your R session:"
echo "   .libPaths(c(\"${R_LIBS_USER}\", .libPaths()))"
echo