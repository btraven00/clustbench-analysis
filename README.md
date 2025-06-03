# ClustBench Analysis Scripts

## Overview

This repo provides quick scripts for analyzing the results of clustering algorithm benchmarks produced by the ClustBench omni benchmark. It includes:

- Python script for aggregating scores from multiple runs
- R functions for loading and processing benchmark data
- R Markdown templates for detailed analysis and visualization
- Debugging utilities for troubleshooting R/Nix integration

## Getting Started

### Prerequisites

- R (>= 4.0.0)
- Python (>= 3.8)
- Required R packages:
  - tidyverse
  - arrow
  - patchwork
  - knitr
  - kableExtra
  - DT
  - flexdashboard (for dashboard)
  - plotly (for interactive visualizations)
  - shiny (for dashboard)
  - scales
  - testthat (for running tests)

#### Option 1: Manual Installation

Install required R packages:

```r
install.packages(c("tidyverse", "arrow", "patchwork", "knitr",
                  "kableExtra", "DT", "flexdashboard", "plotly",
                  "shiny", "scales", "testthat"))
```

#### Option 2: Using Nix Shell (Recommended)

A `shell.nix` file is provided to create a reproducible environment with all dependencies:

1. Make sure you have Nix installed: https://nixos.org/download.html
2. Use the provided startup script to automatically enter a Nix shell and start RStudio:
   ```bash
   cd clustbench-analysis
   ./nix-start-rstudio.sh
   ```

The script will:
- Enter a Nix shell environment automatically
- Configure R to find all the Nix-installed packages
- Launch RStudio with the correct environment settings
- Open the project file if available

This method ensures that all dependencies are available to RStudio automatically.

### Data Preparation

1. Run the aggregation script to process raw benchmark results:
   ```bash
   python3 00_aggregate_scores.py --format both path/to/run_directory
   ```

   This will generate `.csv` and `.parquet` files containing the aggregated benchmark results.

2. For multiple runs, use:
   ```bash
   python3 00_aggregate_scores.py --format both --cores 4 path/to/run_dir_1 path/to/run_dir_2 ...
   ```

### Using the Analysis Templates

1. Open the project in RStudio:
   ```bash
   # Simply run the starter script (recommended):
   cd clustbench-analysis
   ./nix-start-rstudio.sh

   # Or without Nix:
   rstudio clustbench-analysis.Rproj
   ```

   The `nix-start-rstudio.sh` script will verify that R packages are correctly configured before launching RStudio.

2. Choose one of the R Markdown templates:
   - `analysis_template.Rmd` - For comprehensive analysis
   - `dashboard_template.Rmd` - For interactive exploration

3. Knit the R Markdown file to generate the report or dashboard:
   - Click the "Knit" button in RStudio, or
   - Use the `rmarkdown::render()` function in R

## File Structure

- `00_aggregate_scores.py`: Script for aggregating raw benchmark results
- `01_load_data.R`: Helper functions for loading and processing data
- `02_aggregation.R`: Helper functions for aggregating by dataset x method x metric
- `10_analysis.Rmd`: Notebook for benchmark analysis
- `20_dashboard.Rmd`: Interactive notebook for exploring results
- `tests/`: Unit tests for the helper R functions

## Analysis Capabilities

The templates include code for:

- Basic statistics about the benchmark results
- Score comparisons across backends, methods, and datasets
- Performance analysis (execution time)
- Performance vs. quality trade-offs
- Anomaly detection in benchmark results
- Interactive data exploration (dashboard)

## Running Tests

To run the unit tests:

```bash
cd clustbench-analysis
Rscript run_tests.R
```

## Contributing

Contributions are welcome! Please feel free to submit a pull request.
