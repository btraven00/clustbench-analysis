#!/bin/bash
# docker-entrypoint.sh - Entrypoint script for the clustbench-analysis Docker image
set -e

# Debug function
debug_env() {
  echo "==== DEBUG INFO ===="
  echo "Current user: $(whoami)"
  echo "Working dir: $(pwd)"
  echo "Directory permissions:"
  ls -la /tmp /app
  echo "Environment variables:"
  env | sort
  echo "===================="
}

# Ensure directories exist with proper permissions
mkdir -p /app /tmp/R_libs_user /tmp/Rtmp
chmod -R 777 /tmp /tmp/R_libs_user /tmp/Rtmp

# Set up R temp directories
export R_LIBS_USER="/tmp/R_libs_user"
export TMPDIR="/tmp/Rtmp"
export TMP="/tmp/Rtmp"
export TEMP="/tmp/Rtmp"
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

# Copy .Rprofile to app directory if needed
if [ -f /.Rprofile ] && [ ! -f /app/.Rprofile ]; then
  cp /.Rprofile /app/.Rprofile
fi

# Print basic environment information
echo "=== Clustbench Analysis Environment ==="
echo "R version: $(R --version | head -n 1)"
echo "Python version: $(python --version)"
echo "Working directory: $(pwd)"

# Change to /app directory if mounted
if [ -d /app ] && [ "$(ls -A /app 2>/dev/null)" ]; then
  cd /app
  echo "Changed to directory: /app"
fi

# Handle special commands
case "$1" in
  bash|sh|"")
    echo -e "\nWelcome to the Clustbench Analysis container!"
    echo "Available commands:"
    echo "  * R - Run R interactive shell"
    echo "  * Rscript path/to/script.R - Run R script"
    echo "  * python path/to/script.py - Run Python script"
    echo "  * clustbench-test - Run project tests"
    echo "  * debug-env - Show debugging information"
    echo ""
    exec bash
    ;;
  debug-env)
    debug_env
    exit 0
    ;;
  clustbench-test)
    echo "Running tests for clustbench-analysis"
    
    if [ -f run_tests.R ]; then
      Rscript run_tests.R
      exit $?
    else
      echo "Error: run_tests.R not found in $(pwd)"
      ls -la
      exit 1
    fi
    ;;
  *)
    # Execute the command
    exec "$@"
    ;;
esac