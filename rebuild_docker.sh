#!/usr/bin/env bash
#
# rebuild_docker.sh - Rebuild the Docker image for clustbench-analysis
#

set -e

echo "=== Rebuilding clustbench-analysis Docker image ==="

# Check if nix-build exists
if ! command -v nix-build &>/dev/null; then
  echo "Error: nix-build not found. Please install Nix."
  exit 1
fi

# Step 1: Clean old builds if they exist
echo "Cleaning old build results..."
rm -f result

# Step 2: Build the Docker image
echo "Building Docker image..."
nix-build docker.nix -A image

# Step 3: Load the image into Docker
echo "Loading Docker image..."
docker load < result

# Step 4: Build the run script
echo "Building run script..."
nix-build docker.nix -A run

echo
echo "=== Build complete! ==="
echo "Run the container:       ./result/bin/run-clustbench-docker"
echo "Verify R packages:       ./result/bin/run-clustbench-docker Rscript verify_docker_deps.R"
echo "Start R in container:    ./result/bin/run-clustbench-docker R"
echo