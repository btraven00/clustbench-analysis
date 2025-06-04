#!/bin/bash
# debug-docker.sh - Debugging script for Docker container issues in clustbench-analysis
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}====== Clustbench Analysis Docker Debug Tool ======${NC}"
echo "This script will help identify common Docker container issues."

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker not found. Please install Docker first.${NC}"
    exit 1
fi

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo -e "${RED}Error: Docker daemon is not running. Please start Docker first.${NC}"
    exit 1
fi

echo -e "${GREEN}Docker is running correctly.${NC}"

# Check if the image exists
if ! docker image inspect clustbench-analysis:latest &> /dev/null; then
    echo -e "${YELLOW}Warning: clustbench-analysis:latest image not found.${NC}"
    echo "Building Docker image first..."
    
    if [ -f ./docker.nix ]; then
        nix-build docker.nix -A image
        docker load < result
    elif [ -f ./nix/docker.nix ]; then
        nix-build ./nix/docker.nix
        docker load < result
    else
        echo -e "${RED}Error: Cannot find docker.nix. Make sure you're in the project root directory.${NC}"
        exit 1
    fi
fi

echo -e "${GREEN}Docker image is available.${NC}"

echo -e "\n${BLUE}=== Testing with different network settings ===${NC}"

echo -e "\n${YELLOW}1. Testing with no network (--network=none):${NC}"
docker run --rm --network=none clustbench-analysis:latest debug-env

echo -e "\n${YELLOW}2. Testing with host network (--network=host):${NC}"
docker run --rm --network=host clustbench-analysis:latest debug-env

# Try different volume mounting options
echo -e "\n${BLUE}=== Testing volume mounting ===${NC}"
TESTDIR=$(mktemp -d)
echo "Creating test file in: $TESTDIR"
echo "Test content" > "$TESTDIR/test.txt"

echo -e "\n${YELLOW}Testing with volume mount:${NC}"
docker run --rm --network=none -v "$TESTDIR:/app" clustbench-analysis:latest ls -la /app

# Testing R specifically
echo -e "\n${BLUE}=== Testing R environment ===${NC}"
echo -e "${YELLOW}Basic R test:${NC}"
docker run --rm --network=none clustbench-analysis:latest R --version

echo -e "\n${YELLOW}R tempdir test:${NC}"
docker run --rm --network=none clustbench-analysis:latest R -e 'cat("Temp dir:", tempdir(), "\n"); cat("Writable:", file.exists(tempdir()) || dir.create(tempdir(), recursive=TRUE), "\n"); writeLines("test", file.path(tempdir(), "test.txt")); cat("Write successful:", file.exists(file.path(tempdir(), "test.txt")), "\n")'

# Clean up
rm -rf "$TESTDIR"

echo -e "\n${BLUE}======= Debug Complete =======${NC}"
echo "If tests pass but your main use case is still failing, try running with:"
echo -e "${GREEN}docker run -it --rm --network=none -v \"\$(pwd):/app\" clustbench-analysis:latest bash${NC}"
echo "and then manually test your specific use case."

echo -e "\n${YELLOW}Common issues:${NC}"
echo "1. Networking issues: Try using --network=none or --network=host"
echo "2. Volume mount issues: Ensure the host directory exists and has proper permissions"
echo "3. Temp directory issues: Check if R can create and write to a temp directory"
echo "4. SELinux/AppArmor: Try adding the :z or :Z suffix to volume mounts if on SELinux systems"