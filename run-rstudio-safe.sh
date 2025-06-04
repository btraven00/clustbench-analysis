#!/usr/bin/env bash
#
# run-rstudio-safe.sh - Launch RStudio with safe graphics settings
# This script disables GPU acceleration to avoid common graphics issues in Nix environments
#

# Disable OpenGL for RStudio to prevent GPU crashes
export RSTUDIO_CHROMIUM_ARGUMENTS="--disable-gpu --disable-software-rasterizer"
export LIBGL_ALWAYS_SOFTWARE=1
export QT_QPA_PLATFORM=xcb

# Check if we're in a Nix shell
if [[ ! -v IN_NIX_SHELL ]]; then
  echo "Not in a Nix shell, entering one now..."
  exec nix-shell --run "$0 $*"
  exit $?
fi

echo "Starting RStudio with safe graphics settings..."
echo "- GPU acceleration disabled"
echo "- Software rendering enabled"
echo "- Using XCB platform"

# Find the project file if it exists
PROJECT_FILE=""
if [ -f "$(pwd)/clustbench-analysis.Rproj" ]; then
  PROJECT_FILE="$(pwd)/clustbench-analysis.Rproj"
  echo "Opening project: $(basename "$PROJECT_FILE")"
fi

# Launch RStudio with the project if found
if [ -n "$PROJECT_FILE" ]; then
  rstudio "$PROJECT_FILE" "$@" &
else
  rstudio "$@" &
fi

# Don't wait for RStudio to finish
disown

echo "RStudio launched in background"