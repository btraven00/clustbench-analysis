#!/usr/bin/env python3
"""
Script to aggregate clustbench.scores.gz files from the output tree,
extracting dataset generator, dataset name, method, metric, scores,
performance time (seconds), true k value, and noise presence.

Usage:
  python aggregate_scores.py --root <output_directory> [--format csv|parquet|both] [--debug]

Examples:
  python aggregate_scores.py --format both out_apptainer-202505301205
  python aggregate_scores.py out_apptainer-202505301205 --format parquet
  python aggregate_scores.py out_apptainer-202505301205 --format both --debug
"""

import argparse
import csv
import glob
import gzip
import json
import os
import re
import warnings
import multiprocessing

from collections import defaultdict
from functools import partial
from pathlib import Path

import pandas as pd
import numpy as np

# Define a worker function for the process pool
def process_dir(base_dir, debug_mode):
    return process_run(base_dir, debug_mode)


def process_run(base_dir, debug_mode=False):
    """
    Process a run directory containing clustbench results.
    
    This function handles both individual run directories (out_BACKEND-TIMESTAMP)
    and parent directories containing multiple run directories.
    """
    all_results_from_base_dir = []
    all_duplicate_k_anomaly_files_from_base_dir = []
    backends_found = set()
    timestamps_found = set()

    # First check if the input directory is itself a run directory
    base_dir_name = os.path.basename(os.path.normpath(base_dir))
    if base_dir_name.startswith('out_') and '-' in base_dir_name:
        run_dirs = [base_dir]
    else:
        # If not, look for run directories within it
        run_dirs_pattern = os.path.join(base_dir, 'out_*-*')
        run_dirs = glob.glob(run_dirs_pattern)

    if debug_mode:
        print(f"Processing directory: {base_dir}")
        print(f"Found run directories: {run_dirs}")

    if not run_dirs:
        print(f"No run directories found in or matching {base_dir}")
        return [], None, None, [], os.path.basename(os.path.normpath(base_dir))

    for run_dir in run_dirs:
        # Extract backend and timestamp from the run directory name
        backend, timestamp = extract_backend_timestamp(run_dir)
        dir_name = os.path.basename(os.path.normpath(run_dir)) # Use run_dir name as source_dir

        if backend and timestamp:
            print(f"Processing run directory: {run_dir} (Backend: {backend}, Timestamp: {timestamp})")
            backends_found.add(backend)
            timestamps_found.add(timestamp)

            # Find all clustbench.scores.gz files within this specific run directory
            score_files = glob.glob(f'{run_dir}/**/clustbench.scores.gz', recursive=True)
            print(f"Found {len(score_files)} score files in {run_dir}")

            # Process each file and collect results for this run directory
            results_from_run_dir = []
            dataset_info_cache = {} # Cache is per run directory

            for i, file_path in enumerate(score_files):
                if debug_mode and i % 100 == 0 and i > 0:
                     print(f"  Processing {i}/{len(score_files)} files in {run_dir}...")

                # Pass backend and timestamp to process_scores_file if needed, or ensure it's added later
                # process_scores_file extracts info from the file_path itself,
                # backend and timestamp are linked to the run_dir containing the file_path
                result = process_scores_file(file_path)

                if result:
                    # Get dataset generator and name from the result (extracted from file_path)
                    dataset_gen = result['dataset_generator']
                    dataset_name = result['dataset_name']

                    # Use cache for true_k and has_noise (cache is per run_dir)
                    cache_key = f"{dataset_gen}_{dataset_name}"
                    if cache_key not in dataset_info_cache:
                        # Pass the current run_dir to extract_dataset_true_k_and_noise
                        true_k, has_noise = extract_dataset_true_k_and_noise(run_dir, dataset_gen, dataset_name)
                        dataset_info_cache[cache_key] = (true_k, has_noise)
                        if debug_mode:
                            print(f"  Dataset {dataset_gen}_{dataset_name} in {run_dir}: true_k={true_k}, has_noise={has_noise}")
                    else:
                        true_k, has_noise = dataset_info_cache[cache_key]

                    # Add true_k, has_noise, source directory, backend and timestamp to the result
                    # These are specific to the current run_dir
                    result['true_k'] = true_k
                    result['has_noise'] = has_noise
                    result['source_dir'] = dir_name # Name of the run directory (e.g., out_apptainer-TIMESTAMP)
                    result['backend'] = backend     # Backend from run directory name
                    result['run_timestamp'] = timestamp # Timestamp from run directory name

                    # Extract score for k=true_k if available
                    # This logic is already in process_scores_file, but we ensure 'score' key exists
                    # Even if true_k score is missing, process_scores_file should return None for 'score'
                    # No need to re-calculate score_for_true_k here if process_scores_file already does it.
                    # Ensure 'score' key is present even if processing failed partially
                    if 'score' not in result:
                         score_for_true_k = None
                         if true_k is not None:
                             true_k_str = f"k={true_k}"
                             if true_k_str in result and result[true_k_str] is not None:
                                 score_for_true_k = result[true_k_str]
                         result['score'] = score_for_true_k # Add or update score

                    # Ensure consistency flags are present
                    if 'missing_true_k_score' not in result:
                         result['missing_true_k_score'] = (true_k is not None and result.get('score') is None)
                    if 'empty_file' not in result:
                         result['empty_file'] = False # Should be set by process_scores_file if file was empty

                    # Collect results for this run directory
                    results_from_run_dir.append(result)

            # Extend the overall results list with results from this run directory
            all_results_from_base_dir.extend(results_from_run_dir)
            # Note: duplicate_k_anomaly_files were handled within process_scores_file now

        else:
            print(f"Warning: Skipping directory {run_dir} as it does not match expected format 'out_BACKEND-TIMESTAMP'")

    # Return aggregated results from all run directories within this base_dir
    # We can return the first found backend/timestamp or None if none were found.
    # For simplicity, let's convert the sets to sorted lists and return the first element or None.
    backend_list = sorted(list(backends_found))
    timestamp_list = sorted(list(timestamps_found))

    # Collect duplicate_k_anomaly_files from all results
    # We need to re-collect these as they are now part of the result dictionary
    all_duplicate_k_anomaly_files_from_base_dir = [
        res['file_path'] # Assuming we add file_path to result in process_scores_file
        for res in all_results_from_base_dir if res.get('duplicate_k_anomaly', False) and 'file_path' in res
    ]


    # Return aggregated results, a representative backend/timestamp, aggregated anomaly files, and original base_dir name
    return all_results_from_base_dir, \
           backend_list[0] if backend_list else None, \
           timestamp_list[0] if timestamp_list else None, \
           all_duplicate_k_anomaly_files_from_base_dir, \
           os.path.basename(os.path.normpath(base_dir))

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
    # Debug info
    debug_info = {"file": file_path}

    try:
        # Navigate up from score file to find the method directory
        # Scores are in: .../method-XXX/metrics/partition_metrics/metric-YYY/clustbench.scores.gz

        # Start with the metric directory
        current_dir = os.path.dirname(file_path)
        debug_info["metric_dir"] = current_dir

        # Find the method directory by navigating upwards until we find a directory with 'method-' in its name
        method_dir = None
        max_levels = 10  # Safety to avoid infinite loops
        levels = 0
        test_dir = current_dir

        while levels < max_levels:
            # Check if we're at method level
            basename = os.path.basename(test_dir)
            if basename.startswith('method-') or 'method-' in basename:
                method_dir = test_dir
                debug_info["found_at_level"] = levels
                break

            # If we reach the root directory, stop
            if test_dir == os.path.dirname(test_dir):
                debug_info["reached_root"] = True
                break

            # Go up one level
            test_dir = os.path.dirname(test_dir)
            levels += 1

        # If we couldn't find a method directory, try the alternative approach
        if method_dir is None:
            debug_info["first_approach_failed"] = True
            # Try the previous approach as fallback
            if "method-" in current_dir:
                method_dir = current_dir
                debug_info["fallback_direct"] = True
            else:
                try:
                    # Navigate up to partition_metrics
                    partition_metrics_dir = os.path.dirname(current_dir)
                    debug_info["partition_metrics_dir"] = partition_metrics_dir
                    # Navigate up to metrics
                    metrics_dir = os.path.dirname(partition_metrics_dir)
                    debug_info["metrics_dir"] = metrics_dir
                    # Navigate up to method
                    method_dir = os.path.dirname(metrics_dir)
                    debug_info["fallback_path"] = True
                except Exception as e:
                    debug_info["fallback_error"] = str(e)
                    # If this fails, we'll return None at the end
                    pass

        # If we still don't have a method directory, give up
        if method_dir is None:
            debug_info["no_method_dir"] = True
            print(f"Could not find method directory for {file_path}")
            return None

        debug_info["method_dir_before"] = method_dir

        # Check for symlinks in the method directory path and resolve them if needed
        if os.path.islink(method_dir):
            debug_info["is_symlink"] = True
            method_dir = os.path.realpath(method_dir)
            debug_info["method_dir_after"] = method_dir

        # Check for the performance file at the method level
        perf_file = os.path.join(method_dir, 'clustbench_performance.txt')
        debug_info["perf_file"] = perf_file

        if os.path.exists(perf_file):
            debug_info["perf_file_exists"] = True
            with open(perf_file, 'r') as f:
                # Read the header line to get column positions
                header = f.readline().strip().split('\t')
                debug_info["header"] = header
                # Read the data line
                data_line = f.readline().strip()
                debug_info["data_line"] = data_line
                if data_line:
                    data = data_line.split('\t')
                    debug_info["data"] = data

                    # Find the 's' (seconds) column index
                    if 's' in header:
                        s_index = header.index('s')
                        debug_info["s_index"] = s_index
                        if s_index < len(data):
                            try:
                                time_value = float(data[s_index])
                                debug_info["time_value"] = time_value
                                # Print debug info for a sample of files
                                if hash(file_path) % 1000 == 0:
                                    print(f"DEBUG: {debug_info}")
                                return time_value
                            except ValueError:
                                debug_info["value_error"] = True
                        else:
                            debug_info["index_out_of_range"] = True
                    else:
                        debug_info["no_s_column"] = True
                else:
                    debug_info["empty_data_line"] = True
        else:
            debug_info["perf_file_missing"] = True

    except Exception as e:
        debug_info["exception"] = str(e)
        print(f"Error reading method performance file for {file_path}: {e}")

    # Print debug info for cases where we didn't find a time
    print(f"DEBUG (no time): {debug_info}")
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
    # Debug processing every 1000th file to avoid too much output
    debug_this_file = hash(file_path) % 1000 == 0
    try:
        # Extract information from the path
        dataset_gen, dataset_name = extract_dataset_info(file_path)
        method = extract_method_info(file_path)
        metric = extract_metric_info(file_path)

        # Extract performance time (seconds) from the method directory
        execution_time = find_method_performance(file_path)
        if debug_this_file:
            print(f"DEBUG processing {file_path}, found execution_time: {execution_time}")

        # Read the gzipped CSV file
        with gzip.open(file_path, 'rt') as f:
            # Check if the file is empty
            content = f.read()
            if not content.strip():
                print(f"Warning: Empty file encountered: {file_path}")
                # Create result with NAs for missing values
                return {
                    'dataset_generator': dataset_gen,
                    'dataset_name': dataset_name,
                    'method': method,
                    'metric': metric,
                    'execution_time_seconds': execution_time,
                    'duplicate_k_anomaly': False,
                    'score': None,  # NA for score
                    'empty_file': True,
                    'missing_true_k_score': False
                }

            # Reset file pointer to beginning
            f.seek(0)

            # Read the header and data
            reader = csv.reader(f)
            try:
                header = next(reader)
                data_rows = list(reader)
            except StopIteration:
                print(f"Warning: CSV file has no header or data: {file_path}")
                # Create result with NAs for missing values
                return {
                    'dataset_generator': dataset_gen,
                    'dataset_name': dataset_name,
                    'method': method,
                    'metric': metric,
                    'execution_time_seconds': execution_time,
                    'duplicate_k_anomaly': False,
                    'score': None,  # NA for score
                    'empty_file': True,
                    'missing_true_k_score': False
                }

            if len(data_rows) == 0:
                # Create result with NAs for missing values
                return {
                    'dataset_generator': dataset_gen,
                    'dataset_name': dataset_name,
                    'method': method,
                    'metric': metric,
                    'execution_time_seconds': execution_time,
                    'duplicate_k_anomaly': False,
                    'score': None,  # NA for score
                    'empty_file': True,
                    'missing_true_k_score': False
                }

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
                'duplicate_k_anomaly': duplicate_k_anomaly,
                'empty_file': False,
                'missing_true_k_score': False
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
        import traceback
        error_details = traceback.format_exc()
        print(f"Error processing {file_path}: {e}")
        print(f"Full error details:\n{error_details}")
        # Continue processing other files
        return None

def extract_backend_timestamp(directory_name):
    """Extract backend and timestamp from directory name"""
    # Expected format: out_BACKEND-TIMESTAMP
    match = re.match(r'out_([^-]+)-(\d+)', os.path.basename(directory_name))
    if match:
        return match.group(1), match.group(2)
    # Return None, None if the pattern doesn't match
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
    parser.add_argument('--cores', '-c', type=int, default=multiprocessing.cpu_count(),
                        help='Number of CPU cores to use for parallel processing (default: all available cores)')
    args = parser.parse_args()

    all_results = []
    all_backends = []
    all_timestamps = []
    all_duplicate_k_anomaly_files = []
    source_dirs = []

    # If multiple cores are requested and we have multiple directories, use parallel processing
    if args.cores > 1 and len(args.root_dirs) > 1:
        print(f"Using {args.cores} CPU cores for parallel processing...")
        # Create a process pool with the specified number of cores
        with multiprocessing.Pool(processes=args.cores) as pool:
            # Process each directory in parallel
            process_results = pool.map(partial(process_dir, debug_mode=args.debug), args.root_dirs)

            # Collect results
            for result_tuple in process_results:
                results, backend, timestamp, duplicate_k_anomaly_files, dir_name = result_tuple
                if results:
                    all_results.extend(results)
                    if backend not in all_backends:
                        all_backends.append(backend)
                    if timestamp not in all_timestamps:
                        all_timestamps.append(timestamp)
                    all_duplicate_k_anomaly_files.extend(duplicate_k_anomaly_files)
                    source_dirs.append(dir_name)
    else:
        # Process sequentially if only one core is requested or if there's only one directory
        if args.cores == 1:
            print("Using single-core processing...")
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
        meta_columns = ['source_dir', 'backend', 'run_timestamp', 'dataset_generator', 'dataset_name', 'true_k', 'has_noise', 'method', 'metric', 'score', 'execution_time_seconds', 'duplicate_k_anomaly', 'empty_file', 'missing_true_k_score']
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

        # Count empty files and missing true_k scores for reporting
        empty_file_count = sum(1 for result in all_results if result.get('empty_file', False))
        missing_true_k_count = sum(1 for result in all_results if result.get('missing_true_k_score', False))

        if empty_file_count > 0:
            print(f"Warning: Found {empty_file_count} empty files out of {len(all_results)} total files")

        if missing_true_k_count > 0:
            print(f"Warning: Found {missing_true_k_count} entries missing true_k score out of {len(all_results)} total files")

        # Count empty files and missing scores for reporting
        empty_file_count = df['empty_file'].sum()
        missing_true_k_score_count = df['missing_true_k_score'].sum()

        if empty_file_count > 0:
            print(f"Warning: Found {empty_file_count} empty files out of {len(df)} total records")

        if missing_true_k_score_count > 0:
            print(f"Warning: Found {missing_true_k_score_count} records with missing true_k score values")

        # Count empty files and missing true_k scores for reporting
        empty_file_count = sum(1 for result in all_results if result.get('empty_file', False))
        missing_true_k_count = sum(1 for result in all_results if result.get('missing_true_k_score', False))

        if empty_file_count > 0:
            print(f"Warning: Found {empty_file_count} empty files out of {len(all_results)} total files")

        if missing_true_k_count > 0:
            print(f"Warning: Found {missing_true_k_count} entries missing true_k score out of {len(all_results)} total files")

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
