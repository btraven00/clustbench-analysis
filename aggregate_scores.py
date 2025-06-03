#!/usr/bin/env python3
"""
Script to aggregate clustbench.scores.gz files from the output tree,
extracting dataset generator, dataset name, method, metric, scores,
performance time (seconds), true k value, and noise presence.

Usage:
  python aggregate_scores.py --root <output_directory> [--format csv|parquet|both] [--debug]

Examples:
  python aggregate_scores.py --root out_apptainer-202505301205
  python aggregate_scores.py --root out_apptainer-202505301205 --format parquet
  python aggregate_scores.py --root out_apptainer-202505301205 --format both --debug
"""

import os
import glob
import gzip
import json
import pandas as pd
import re
import csv
import argparse
from collections import defaultdict
import numpy as np
from pathlib import Path
import warnings


def process_run(base_dir, debug_mode=False):
    # Extract backend and timestamp
    backend, timestamp = extract_backend_timestamp(base_dir)
    print(f"Detected backend: {backend}, timestamp: {timestamp}")

    # Get directory name for tracking source
    dir_name = os.path.basename(os.path.normpath(base_dir))

    # Find all clustbench.scores.gz files
    score_files = glob.glob(f'{base_dir}/**/clustbench.scores.gz', recursive=True)
    print(f"Found {len(score_files)} score files")

    # Process each file and collect results
    results = []

    # Cache for true_k and has_noise values to avoid repeated file reads
    dataset_info_cache = {}

    # Track files with duplicate k anomalies
    duplicate_k_anomaly_files = []

    for i, file_path in enumerate(score_files):
        if i % 100 == 0 and i > 0:
            print(f"Processed {i}/{len(score_files)} files...")
        result = process_scores_file(file_path)
        if result:
            # Get dataset generator and name from the result
            dataset_gen = result['dataset_generator']
            dataset_name = result['dataset_name']

            # Use cache to avoid repeated file reads
            cache_key = f"{dataset_gen}_{dataset_name}"
            if cache_key not in dataset_info_cache:
                true_k, has_noise = extract_dataset_true_k_and_noise(base_dir, dataset_gen, dataset_name)
                dataset_info_cache[cache_key] = (true_k, has_noise)
                if debug_mode:
                    print(f"Dataset {dataset_gen}_{dataset_name}: true_k={true_k}, has_noise={has_noise}")
            else:
                true_k, has_noise = dataset_info_cache[cache_key]

            # Add true_k, has_noise, source directory, backend and timestamp to the result
            result['true_k'] = true_k
            result['has_noise'] = has_noise
            result['source_dir'] = dir_name
            result['backend'] = backend
            result['run_timestamp'] = timestamp

            # Extract score for k=true_k if available
            score_for_true_k = None
            true_k_str = f"k={true_k}"
            if true_k_str in result:
                score_for_true_k = result[true_k_str]
            result['score'] = score_for_true_k

            # Track duplicate k anomalies
            if result.get('duplicate_k_anomaly', False):
                duplicate_k_anomaly_files.append(file_path)
                if debug_mode:
                    print(f"DUPLICATE K ANOMALY DETECTED in file: {file_path}")

            results.append(result)
    return results, backend, timestamp, duplicate_k_anomaly_files, dir_name

def extract_dataset_info(path):
    """Extract dataset generator and name from path or parameters.json"""
    # Try to get from directory name
    dir_match = re.search(r'dataset_generator-(\w+)_dataset_name-(\w+)', path)
    if dir_match:
        return dir_match.group(1), dir_match.group(2)

    # Try to get from parameters.json
    params_file = os.path.join(os.path.dirname(path), 'parameters.json')
    if os.path.exists(params_file):
        with open(params_file, 'r') as f:
            try:
                params = json.load(f)
                return params.get('dataset_generator', ''), params.get('dataset_name', '')
            except:
                pass

    # Go up one level and try again
    parent_dir = os.path.dirname(os.path.dirname(path))
    if parent_dir == path:  # Stop recursion at root
        return '', ''
    return extract_dataset_info(parent_dir)

def extract_method_info(path):
    """Extract method name from path or parameters.json"""
    # Try to get from directory name
    dir_match = re.search(r'method-(\w+)', path)
    if dir_match:
        return dir_match.group(1)

    # Find the method directory
    current_dir = os.path.dirname(path)
    # Try to navigate up to find method directory
    for _ in range(5):  # Limit recursion depth
        if current_dir == os.path.dirname(current_dir):  # Reached root
            break

        # Look for parameters.json in the current directory
        params_file = os.path.join(current_dir, 'parameters.json')
        if os.path.exists(params_file):
            with open(params_file, 'r') as f:
                try:
                    params = json.load(f)
                    method = params.get('method', '')
                    if method:
                        return method
                except:
                    pass

        # Look for method in directory name
        if os.path.basename(current_dir).startswith('method-'):
            return os.path.basename(current_dir).split('-')[1]

        # Check for clustering library and linkage method pattern
        base_dir = os.path.basename(current_dir)
        if base_dir.startswith('linkage-'):
            parent_dir = os.path.basename(os.path.dirname(current_dir))
            if parent_dir in ['agglomerative', 'fastcluster', 'sklearn']:
                return f"{parent_dir}_{base_dir}"

        # Move up one directory
        current_dir = os.path.dirname(current_dir)

    # If we still don't have a method, check the path components directly
    path_parts = path.split(os.sep)
    for i, part in enumerate(path_parts):
        if part.startswith('method-'):
            return part.split('-')[1]
        if part.startswith('linkage-') and i > 0:
            if path_parts[i-1] in ['agglomerative', 'fastcluster', 'sklearn']:
                return f"{path_parts[i-1]}_{part}"

    return ''  # Return empty string if method can't be found

def extract_metric_info(path):
    """Extract metric name from path or parameters.json"""
    # Try to get from directory name
    dir_match = re.search(r'metric-(\w+)', path)
    if dir_match:
        return dir_match.group(1)

    # Try to get from parameters.json
    params_file = os.path.join(os.path.dirname(path), 'parameters.json')
    if os.path.exists(params_file):
        with open(params_file, 'r') as f:
            try:
                params = json.load(f)
                return params.get('metric', '')
            except:
                pass

    return ''

def find_method_performance(file_path):
    """Find and extract execution time (seconds) from method's clustbench_performance.txt"""
    try:
        # Navigate up from score file to find the method directory
        # Scores are in: .../method-XXX/metrics/partition_metrics/metric-YYY/clustbench.scores.gz
        current_dir = os.path.dirname(file_path)  # metric-YYY directory

        # If we're already at the method level, use this directory
        if "method-" in current_dir:
            method_dir = current_dir
        else:
            # Navigate up to partition_metrics
            partition_metrics_dir = os.path.dirname(current_dir)
            # Navigate up to metrics
            metrics_dir = os.path.dirname(partition_metrics_dir)
            # Navigate up to method
            method_dir = os.path.dirname(metrics_dir)

        # Check for the performance file
        perf_file = os.path.join(method_dir, 'clustbench_performance.txt')

        if os.path.exists(perf_file):
            with open(perf_file, 'r') as f:
                # Read the header line to get column positions
                header = f.readline().strip().split('\t')
                # Read the data line
                data_line = f.readline().strip()
                if data_line:
                    data = data_line.split('\t')

                    # Find the 's' (seconds) column index
                    if 's' in header:
                        s_index = header.index('s')
                        if s_index < len(data):
                            try:
                                return float(data[s_index])
                            except ValueError:
                                pass
    except Exception as e:
        print(f"Error reading method performance file for {file_path}: {e}")

    return None

def process_scores_file(file_path):
    """
    Process a clustbench.scores.gz file and extract relevant data

    This function:
    1. Extracts dataset, method and metric information from the path
    2. Reads the performance time from the method's performance file
    3. Parses the scores file to extract k-value scores
    4. Detects duplicate k values with significantly different scores
    """
    try:
        # Extract information from the path
        dataset_gen, dataset_name = extract_dataset_info(file_path)
        method = extract_method_info(file_path)
        metric = extract_metric_info(file_path)

        # Extract performance time (seconds) from the method directory
        execution_time = find_method_performance(file_path)

        # Read the gzipped CSV file
        with gzip.open(file_path, 'rt') as f:
            # Read the header and data
            reader = csv.reader(f)
            header = next(reader)
            data_rows = list(reader)

            if len(data_rows) == 0:
                return None

            # Process the data
            data = data_rows[0]  # Assuming single row of values

            # Check for duplicate k values with different results
            # Check for duplicate k anomalies
            duplicate_k_anomaly = False
            k_values = {}
            for i, k in enumerate(header):
                if i < len(data):
                    k_cleaned = k.strip('"')  # Remove quotes if present
                    try:
                        value = float(data[i])
                        if k_cleaned in k_values:
                            # If same k appears multiple times, check if values differ significantly
                            if abs(k_values[k_cleaned] - value) > 1e-3:  # Epsilon of 1E-3
                                duplicate_k_anomaly = True
                                # Debug info collected even without debug flag to populate anomaly report
                                if k_cleaned not in k_values.get('duplicates', {}):
                                    k_values.setdefault('duplicates', {})[k_cleaned] = [(k_values[k_cleaned], value)]
                                else:
                                    k_values['duplicates'][k_cleaned].append((k_values[k_cleaned], value))
                        else:
                            k_values[k_cleaned] = value
                    except (ValueError, TypeError):
                        # Non-numeric values - just store first occurrence
                        if k_cleaned not in k_values:
                            k_values[k_cleaned] = data[i]

            # Create result dictionary
            result = {
                'dataset_generator': dataset_gen,
                'dataset_name': dataset_name,
                'method': method,
                'metric': metric,
                'execution_time_seconds': execution_time,
                'duplicate_k_anomaly': duplicate_k_anomaly
            }

            # Add k values from header and corresponding scores
            for i, k in enumerate(header):
                if i < len(data):
                    k_cleaned = k.strip('"')  # Remove quotes if present
                    try:
                        # Try to convert to float for numerical values
                        result[k_cleaned] = float(data[i])
                    except ValueError:
                        result[k_cleaned] = data[i]

            return result
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def extract_backend_timestamp(directory_name):
    """Extract backend and timestamp from directory name"""
    # Expected format: out_BACKEND-TIMESTAMP
    match = re.match(r'out_([^-]+)-(\d+)', os.path.basename(directory_name))
    if match:
        return match.group(1), match.group(2)
    return None, None

def extract_dataset_true_k_and_noise(base_dir, dataset_gen, dataset_name):
    """
    Extract the true number of clusters and noise presence from dataset labels file.

    Parameters:
    base_dir (str): Base directory for clustbench output
    dataset_gen (str): Dataset generator name
    dataset_name (str): Dataset name

    Returns:
    tuple: (true_k, has_noise) where true_k is the number of unique non-zero labels,
           and has_noise is True if label 0 is present

    Notes:
    - Searches for clustbench.labels*.gz files in hash-named directories
    - Counts unique non-zero labels to determine true_k
    - Checks for presence of label 0 to determine noise
    """
    # Path pattern to match dataset directories with hash names
    pattern = os.path.join(base_dir, 'data', 'clustbench', '.*')
    hash_dirs = glob.glob(pattern)

    for hash_dir in hash_dirs:
        # Check for labels files
        label_files = glob.glob(os.path.join(hash_dir, 'clustbench.labels*.gz'))

        for label_file in label_files:
            try:
                # Read the labels file
                with gzip.open(label_file, 'rt') as f:
                    labels = np.loadtxt(f)

                # Look for parameters.json to match dataset_gen and dataset_name
                params_file = os.path.join(hash_dir, 'parameters.json')
                if os.path.exists(params_file):
                    with open(params_file, 'r') as f:
                        try:
                            params = json.load(f)
                            file_dataset_gen = params.get('dataset_generator', '')
                            file_dataset_name = params.get('dataset_name', '')

                            # If this is our dataset, return the true k and noise info
                            if file_dataset_gen == dataset_gen and file_dataset_name == dataset_name:
                                # Count unique non-zero labels
                                unique_labels = set(int(label) for label in labels)
                                has_noise = 0 in unique_labels

                                # Remove 0 (noise) from the count if present
                                if has_noise:
                                    unique_labels.remove(0)

                                true_k = len(unique_labels)
                                return true_k, has_noise
                        except Exception as e:
                            print(f"Error processing parameters for {label_file}: {e}")
            except Exception as e:
                print(f"Error reading labels file {label_file}: {e}")

    # If we couldn't find or process the file, return None values
    return None, None

def main():
    """
    Main function to aggregate all clustbench.scores.gz files

    This function:
    1. Processes command line arguments for directory and output format
    2. Finds all clustbench.scores.gz files in the specified directory
    3. Extracts scores, true_k values, and performance metrics
    4. Detects anomalies in the data
    5. Outputs aggregated data in CSV and/or Parquet format
    6. Provides a detailed summary report of the data and any anomalies
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Aggregate clustbench scores and detect anomalies from output directory',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('root_dirs', nargs='+', help='Root directories containing clustbench output')
    parser.add_argument('--out_dir', type=str, default='.', help='Output directory for aggregated results')
    parser.add_argument('--debug', '-d', action='store_true',
                        help='Enable debug output (shows detailed file paths and anomaly information)')
    parser.add_argument('--format', '-f', type=str, choices=['csv', 'parquet', 'both'], default='csv',
                        help='Output format: csv (224KB), parquet (70KB), or both (default: csv)')
    args = parser.parse_args()

    all_results = []
    all_backends = []
    all_timestamps = []
    all_duplicate_k_anomaly_files = []
    source_dirs = []

    for base_dir in args.root_dirs:
        results, backend, timestamp, duplicate_k_anomaly_files, dir_name = process_run(base_dir, args.debug)
        if results:
            all_results.extend(results)
            if backend not in all_backends:
                all_backends.append(backend)
            if timestamp not in all_timestamps:
                all_timestamps.append(timestamp)
            all_duplicate_k_anomaly_files.extend(duplicate_k_anomaly_files)
            source_dirs.append(dir_name)

    # Convert to DataFrame
    if all_results:
        df = pd.DataFrame(all_results)

        # Backend and timestamp are already in individual records
        # No need to add them here since we're preserving the original values

        # Organize columns - metadata first, then k values
        all_columns = df.columns.tolist()
        meta_columns = ['source_dir', 'backend', 'run_timestamp', 'dataset_generator', 'dataset_name', 'true_k', 'has_noise', 'method', 'metric', 'score', 'execution_time_seconds', 'duplicate_k_anomaly']
        k_columns = [col for col in all_columns if col not in meta_columns]

        # Sort k columns numerically if they follow 'k=X' pattern
        def extract_k(col):
            if col.startswith('k='):
                try:
                    return int(col.split('=')[1])
                except:
                    return 999999  # Large number for non-numeric values
            return 999999

        k_columns.sort(key=extract_k)

        # Reorder columns
        df = df[meta_columns + k_columns]

        # Base output filename without extension
        if len(source_dirs) == 1:
            # Single directory case
            dir_name = source_dirs[0]
            backend = all_backends[0] if all_backends else None
            timestamp = all_timestamps[0] if all_timestamps else None
            base_output_file = f'clustbench_aggregated_scores_{dir_name}_{backend}_{timestamp}' if backend and timestamp else f'clustbench_aggregated_scores_{dir_name}'
        else:
            # Multiple directory case
            current_timestamp = pd.Timestamp.now().strftime('%Y%m%d%H%M')
            backends_str = '_'.join(all_backends) if len(all_backends) <= 3 else f'{len(all_backends)}_backends'
            base_output_file = f'clustbench_aggregated_multi_{len(source_dirs)}_dirs_{backends_str}_{current_timestamp}'

        # Create output directory if it doesn't exist
        if not os.path.exists(args.out_dir):
            os.makedirs(args.out_dir)
            
        # Output to CSV
        if args.format in ['csv', 'both']:
            csv_output_file = os.path.join(args.out_dir, f"{base_output_file}.csv")
            df.to_csv(csv_output_file, index=False)
            print(f"Aggregated scores saved to {csv_output_file}")

        # Output to Parquet
        if args.format in ['parquet', 'both']:
            parquet_output_file = os.path.join(args.out_dir, f"{base_output_file}.parquet")
            df.to_parquet(parquet_output_file, index=False)
            print(f"Parquet output written to {parquet_output_file}")

        if args.debug and all_duplicate_k_anomaly_files:
            print("\nFiles with Duplicate K Anomaly:")
            for file in all_duplicate_k_anomaly_files:
                print(f"- {file}")
    else:
        print("No matching score files found.  Nothing to do.")


if __name__ == "__main__":
    main()
