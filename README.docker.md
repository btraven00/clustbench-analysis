# Docker Setup for Clustbench Analysis

This project uses Nix to create a reproducible Docker environment for R and Python analysis tasks.

## Quick Start

Build and load the Docker image:

```bash
# Build everything with the helper script
./rebuild_docker.sh

# Or manually:
nix-build docker.nix -A image
docker load < result
nix-build docker.nix -A run
```

Run the Docker container with your current directory mounted:

```bash
./result/bin/run-clustbench-docker
```

This gives you a shell with:
- R with all required packages (tidyverse, patchwork, etc.)
- Python 3.12 with pandas and numpy

## Running Specific Commands

Run R inside the container:

```bash
./result/bin/run-clustbench-docker R
```

Run an R script:

```bash
./result/bin/run-clustbench-docker Rscript your_script.R
```

Run a Python script:

```bash
./result/bin/run-clustbench-docker python your_script.py
```

## Troubleshooting

### If you get missing R package errors:

Verify your R package dependencies:

```bash
./result/bin/run-clustbench-docker Rscript verify_docker_deps.R
```

To fix dependency issues:
1. Add the missing packages to `nix/default.nix` in the `rPackageList`
2. Add critical dependencies (e.g., `xfun` for `knitr`) to `essentialRDeps` in `docker.nix`
3. Rebuild the Docker image with `./rebuild_docker.sh`

### If you get a networking error:

The helper script already uses `--network=none` to avoid common Docker networking issues.
Use `--network=host` as the first argument to enable network access:

```bash
./result/bin/run-clustbench-docker --network=host R
```

### If you get permission errors:

Make sure your current directory is readable/writable by the container:

```bash
chmod -R 755 .
```

## Image Architecture

- The Docker container uses `rWrapper` to include R with all dependencies
- Essential packages and their dependencies are explicitly included
- R environment is configured via a custom `.Rprofile`
- The container works offline by default for reproducibility
- All data processing happens in mounted directories for persistence