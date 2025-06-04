# Clustbench Analysis Nix Environment

This directory contains Nix configuration files for the clustbench-analysis project. These files provide reproducible development and deployment environments with R and Python dependencies.

## Files

- `default.nix` - Core package definitions for R and Python environments
- `docker.nix` - Docker image builder using the same package definitions
- `docker-entrypoint.sh` - Entrypoint script for the Docker container
- `../docker.nix` - Convenience wrapper for building the Docker image

## Development Environment

To set up the development environment:

```bash
# From the project root directory
nix-shell
```

This will give you access to:
- R with all required packages
- Python 3.12 with pandas and numpy
- RStudio (via the rstudio-wrapper command)

## Docker Image

To build the Docker image:

```bash
# From the project root directory
nix-build docker.nix -A image
docker load < result
```

Or use the convenience script:

```bash
nix-build docker.nix -A shell
./result/bin/build-and-load-docker
```

This will create a Docker image named `clustbench-analysis:latest` that includes:
- R with all required packages
- Python 3.12 with pandas and numpy
- An entrypoint script that helps with environment setup

## Using the Docker Image

Run the container:

```bash
docker run -it --rm -v "$(pwd):/app" clustbench-analysis:latest
```

This will mount the current directory as `/app` in the container and start an interactive bash shell.

Run tests in the container:

```bash
docker run -it --rm -v "$(pwd):/app" clustbench-analysis:latest clustbench-test
```

Run a specific R script:

```bash
docker run -it --rm -v "$(pwd):/app" clustbench-analysis:latest Rscript path/to/script.R
```

Run a specific Python script:

```bash
docker run -it --rm -v "$(pwd):/app" clustbench-analysis:latest python path/to/script.py
```

## Adding New Dependencies

1. Edit `default.nix` to add new R or Python packages
2. The changes will automatically be available in both the development environment and Docker image

## Environment Variables

The following environment variables are set in the Docker container:

- `R_HOME` - Path to R installation
- `PYTHONPATH` - Path to Python packages
- `PATH` - Updated to include R and Python executables

## Troubleshooting

If you encounter build errors with the Docker image:

1. Make sure you have Docker installed and running
2. Try clearing the Nix build cache: `nix-collect-garbage -d`
3. Check for adequate disk space
4. Use verbose mode for more information: `nix-build -v docker.nix -A image`

For runtime errors when using the container, check:

1. Proper directory mounting with the `-v` flag
2. File permissions in mounted directories
3. Proper image name and tag (`clustbench-analysis:latest`)