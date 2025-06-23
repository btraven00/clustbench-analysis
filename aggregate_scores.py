#!/usr/bin/env python3
"""
Script to aggregate clustbench.scores.gz files from the output tree,
extracting dataset generator, dataset name, method, metric, scores,
execution time, runtime, true k value, and noise presence.

This version creates two denormalized output files:
1. method-performance.csv: Dataset x Method x Seed level (execution metrics including execution_time_seconds and runtime)
2. metric-performance.csv: Dataset x Method x Seed x Metric level (clustering quality scores with score for k=true_k and metric-level runtime)

Usage:
  python 00_aggregate_scores.py <root_directories> [--format csv|parquet|both] [--debug] [--out_dir OUTPUT_DIR] [--cores CORES]

Examples:
  python aggregate_scores.py out_apptainer-202505301205 --format both
  python aggregate_scores.py out_apptainer-202505301205 --format parquet --debug
  python aggregate_scores.py out_apptainer-202505301205 out_conda-202506231301 --format csv --cores 4
  python aggregate_scores.py out_apptainer-202505301205 --out_dir results/ --format both
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
from typing import Dict, List, Tuple, Optional, Any

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

    Returns:
        Tuple of (method_results, metric_results, backend, timestamp, duplicate_k_anomaly_files, dir_name)
    """
    all_method_results = []
    all_metric_results = []
    all_duplicate_k_anomaly_files = []
    backends_found = set()
    timestamps_found = set()

    # First check if the input directory is itself a run directory
    if os.path.basename(base_dir).startswith('out_') or os.path.basename(base_dir).startswith('out-'):
        run_dirs = [base_dir]
        print(f"Processing single run directory: {base_dir}")
    else:
        # Find all run directories (directories that start with 'out_' or 'out-')
        run_dirs = []
        if os.path.exists(base_dir):
            for item in os.listdir(base_dir):
                item_path = os.path.join(base_dir, item)
                if os.path.isdir(item_path) and (item.startswith('out_') or item.startswith('out-')):
                    run_dirs.append(item_path)

        if not run_dirs:
            print(f"No run directories found in {base_dir}")
            return [], [], None, None, [], os.path.basename(base_dir)

        print(f"Found {len(run_dirs)} run directories in {base_dir}")

    # Process each run directory
    for run_dir in run_dirs:
        print(f"Processing run directory: {run_dir}")

        # Extract backend and timestamp
        backend, timestamp = extract_backend_timestamp(run_dir)
        if backend:
            backends_found.add(backend)
        if timestamp:
            timestamps_found.add(timestamp)

        # Find all clustbench.scores.gz files
        pattern = os.path.join(run_dir, '**/clustbench.scores.gz')
        score_files = glob.glob(pattern, recursive=True)

        print(f"Found {len(score_files)} score files in {run_dir}")

        if debug_mode:
            print(f"Sample score files from {run_dir}:")
            for f in score_files[:3]:  # Show first 3 files
                print(f"  {f}")

        # Process each score file
        for score_file in score_files:
            try:
                method_result, metric_result = process_scores_file(score_file, backend, timestamp, run_dir)

                if method_result:
                    all_method_results.append(method_result)
                if metric_result:
                    all_metric_results.append(metric_result)

                # Check for duplicate k anomaly
                if metric_result and metric_result.get('duplicate_k_anomaly', False):
                    all_duplicate_k_anomaly_files.append(score_file)

            except Exception as e:
                print(f"Error processing {score_file}: {e}")
                continue

    # Return single values for backend and timestamp if only one found
    backend = list(backends_found)[0] if len(backends_found) == 1 else None
    timestamp = list(timestamps_found)[0] if len(timestamps_found) == 1 else None

    return (all_method_results, all_metric_results, backend, timestamp,
            all_duplicate_k_anomaly_files, os.path.basename(base_dir))


def extract_dataset_info(path: str) -> Tuple[str, str]:
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


def extract_method_info(path: str) -> Dict[str, Any]:
    """Extract method name and seed from path or parameters.json"""
    result = {'method': '', 'seed': None}

    def normalize_method_name(name: str) -> str:
        """Normalize method name by replacing hyphens with underscores, except for linkage- prefix"""
        if name.startswith('linkage-'):
            return name  # Keep linkage- as is
        return name.replace('-', '_')

    # Find the method directory
    current_dir = os.path.dirname(path)
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
                        result['method'] = normalize_method_name(method)
                        if 'seed' in params:
                            result['seed'] = int(params['seed'])
                        return result
                except:
                    pass

        # Look for method in directory name with possible seed
        base_dir = os.path.basename(current_dir)

        # Check for method-name_seed-123 pattern
        seed_match = re.search(r'method-(.+?)_seed-(\d+)', base_dir)
        if seed_match:
            result['method'] = normalize_method_name(seed_match.group(1))
            result['seed'] = int(seed_match.group(2))
            return result

        # Just a regular method without seed
        if base_dir.startswith('method-'):
            result['method'] = normalize_method_name(base_dir.split('-', 1)[1])
            return result

        # Check for clustering library and linkage method pattern
        if base_dir.startswith('linkage-'):
            parent_dir = os.path.basename(os.path.dirname(current_dir))
            if parent_dir in ['agglomerative', 'fastcluster', 'sklearn']:
                # Check if linkage part has seed
                linkage_seed_match = re.search(r'linkage-(.+?)_seed-(\d+)', base_dir)
                if linkage_seed_match:
                    linkage_name = f"linkage-{linkage_seed_match.group(1)}"
                    result['method'] = f"{parent_dir}_{linkage_name}"
                    result['seed'] = int(linkage_seed_match.group(2))
                    return result
                else:
                    result['method'] = f"{parent_dir}_{base_dir}"
                    return result

        current_dir = os.path.dirname(current_dir)

    return result


def extract_metric_info(path: str) -> str:
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
                metric = params.get('metric', '')
                if metric:
                    return metric
            except:
                pass

    # If we still don't have a metric, check the path components directly
    """
    path_parts = path.split(os.sep)
    for part in path_parts:
        if part.startswith('metric-'):
            return part.split('-', 1)[1]
    """

    return ''


def extract_performance_data(file_path: str) -> Dict[str, Any]:
    """Extract performance metrics from method's perf.json file"""
    debug_info = {"file": file_path}

    try:
        # Navigate up from score file to find the method directory
        current_dir = os.path.dirname(file_path)
        debug_info["metric_dir"] = current_dir

        # Find the method directory by navigating upwards
        method_dir = None
        max_levels = 10
        levels = 0
        test_dir = current_dir

        while levels < max_levels:
            basename = os.path.basename(test_dir)
            if basename.startswith('method-') or 'method-' in basename:
                method_dir = test_dir
                debug_info["found_at_level"] = levels
                break

            if test_dir == os.path.dirname(test_dir):
                debug_info["reached_root"] = True
                break

            test_dir = os.path.dirname(test_dir)
            levels += 1

        # Fallback approach
        if method_dir is None:
            debug_info["first_approach_failed"] = True
            if "method-" in current_dir:
                method_dir = current_dir
                debug_info["fallback_direct"] = True
            else:
                try:
                    partition_metrics_dir = os.path.dirname(current_dir)
                    debug_info["partition_metrics_dir"] = partition_metrics_dir
                    metrics_dir = os.path.dirname(partition_metrics_dir)
                    debug_info["metrics_dir"] = metrics_dir
                    method_dir = os.path.dirname(metrics_dir)
                    debug_info["fallback_path"] = True
                except Exception as e:
                    debug_info["fallback_error"] = str(e)

        if method_dir is None:
            debug_info["no_method_dir"] = True
            return {
                'runtime': None,
                'threads': None,
                'disk_read': None,
                'disk_write': None,
                'avg_load': None,
                'peak_rss': None
            }

        debug_info["method_dir_before"] = method_dir

        # Resolve symlinks if needed
        if os.path.islink(method_dir):
            debug_info["is_symlink"] = True
            method_dir = os.path.realpath(method_dir)
            debug_info["method_dir_after"] = method_dir

        # Check for the performance file at the method level
        perf_file = os.path.join(method_dir, 'perf.json')
        debug_info["perf_file"] = perf_file

        if os.path.exists(perf_file):
            debug_info["perf_file_exists"] = True
            with open(perf_file, 'r') as f:
                perf_data = json.load(f)
                debug_info["perf_data_keys"] = list(perf_data.keys())

                result = {
                    'runtime': perf_data.get('total_time_secs'),
                    'threads': perf_data.get('max_threads'),
                    'disk_read': perf_data.get('total_disk_read_bytes'),
                    'disk_write': perf_data.get('total_disk_write_bytes'),
                    'avg_load': perf_data.get('avg_cpu_usage'),
                    'peak_rss': perf_data.get('peak_mem_rss_kb')
                }

                # Debug sampling
                if hash(file_path) % 1000 == 0:
                    print(f"DEBUG perf.json: {debug_info}")
                    print(f"DEBUG perf metrics: {result}")

                return result
        else:
            debug_info["perf_file_missing"] = True

    except Exception as e:
        debug_info["exception"] = str(e)
        print(f"Error reading perf.json file for {file_path}: {e}")

    # Debug sampling for missing data
    if hash(file_path) % 1000 == 0:
        print(f"DEBUG (no perf data): {debug_info}")

    return {
        'runtime': None,
        'threads': None,
        'disk_read': None,
        'disk_write': None,
        'avg_load': None,
        'peak_rss': None
    }


def extract_metric_performance_data(file_path: str) -> Dict[str, Any]:
    """Extract performance metrics from metric's perf.json file"""
    try:
        # The metric directory is where the clustbench.scores.gz file is located
        metric_dir = os.path.dirname(file_path)

        # Check for perf.json in the metric directory
        perf_file = os.path.join(metric_dir, 'perf.json')

        if os.path.exists(perf_file):
            with open(perf_file, 'r') as f:
                perf_data = json.load(f)

                return {
                    'runtime': perf_data.get('total_time_secs'),
                    'threads': perf_data.get('max_threads'),
                    'disk_read': perf_data.get('total_disk_read_bytes'),
                    'disk_write': perf_data.get('total_disk_write_bytes'),
                    'avg_load': perf_data.get('avg_cpu_usage'),
                    'peak_rss': perf_data.get('peak_mem_rss_kb')
                }

    except Exception as e:
        print(f"Error reading metric perf.json file for {file_path}: {e}")

    return {
        'runtime': None,
        'threads': None,
        'disk_read': None,
        'disk_write': None,
        'avg_load': None,
        'peak_rss': None
    }


def find_method_performance(file_path: str) -> Optional[float]:
    """Extract execution time from method's clustbench_performance.txt file"""
    try:
        # Navigate from score file to method directory
        # Path: .../method-XXX/metrics/partition_metrics/metric-YYY/clustbench.scores.gz
        # Go up 4 levels: metric-YYY -> partition_metrics -> metrics -> method-XXX
        current_dir = os.path.dirname(file_path)  # metric-YYY
        partition_metrics_dir = os.path.dirname(current_dir)  # partition_metrics
        metrics_dir = os.path.dirname(partition_metrics_dir)  # metrics
        method_dir = os.path.dirname(metrics_dir)  # method-XXX

        # Look for clustbench_performance.txt file in method directory
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
                                execution_time = float(data[s_index])
                                return execution_time
                            except ValueError:
                                return None

    except Exception as e:
        print(f"Error reading performance file for {file_path}: {e}")

    return None


def extract_dataset_true_k_and_noise(file_path: str) -> Tuple[Optional[int], Optional[bool]]:
    """Extract true k value and noise presence ONLY from labels files"""
    try:
        current_dir = os.path.dirname(file_path)

        # Navigate up to find dataset directory
        dataset_dir = None
        max_levels = 10
        levels = 0
        test_dir = current_dir

        while levels < max_levels:
            basename = os.path.basename(test_dir)
            if 'dataset_generator-' in basename and 'dataset_name-' in basename:
                dataset_dir = test_dir
                break
            if test_dir == os.path.dirname(test_dir):
                break
            test_dir = os.path.dirname(test_dir)
            levels += 1

        if dataset_dir is None:
            return None, None

        # Only try to find labels files and count unique non-zero labels
        label_files = glob.glob(os.path.join(dataset_dir, 'clustbench.labels*.gz'))
        for label_file in label_files:
            try:
                with gzip.open(label_file, 'rt') as f:
                    labels = [int(float(line.strip())) for line in f if line.strip()]
                    unique_labels = set(labels)
                    has_noise = 0 in unique_labels

                    # Remove 0 (noise) from the count if present
                    if has_noise:
                        unique_labels.remove(0)

                    true_k = len(unique_labels)
                    if true_k > 0:
                        return true_k, has_noise
            except Exception as e:
                continue

        # If no labels file found, return None for both
        return None, None

    except Exception as e:
        print(f"Error reading dataset parameters for {file_path}: {e}")

    return None, None


def extract_backend_timestamp(run_dir: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract backend and timestamp from run directory name"""
    basename = os.path.basename(run_dir)

    # Match patterns like: out_apptainer-202505301205, out-conda_202506231301
    match = re.match(r'out[_-]([^_-]+)[_-](\d+)', basename)
    if match:
        return match.group(1), match.group(2)

    return None, None


def process_scores_file(file_path: str, backend: Optional[str], timestamp: Optional[str],
                       source_dir: str) -> Tuple[Optional[Dict], Optional[Dict]]:
    """
    Process a clustbench.scores.gz file and extract method and metric data

    Returns:
        Tuple of (method_result, metric_result)
        - method_result: Performance data at method level (one per method run)
        - metric_result: Score data at metric level (one per method-metric combination)
    """
    debug_this_file = hash(file_path) % 1000 == 0

    try:
        # Extract information from the path
        dataset_gen, dataset_name = extract_dataset_info(file_path)
        method_info = extract_method_info(file_path)
        method = method_info['method']
        seed = method_info['seed']
        metric = extract_metric_info(file_path)

        # Extract dataset info
        true_k, has_noise = extract_dataset_true_k_and_noise(file_path)

        # Extract performance time (seconds) from the method directory
        execution_time = find_method_performance(file_path)
        if debug_this_file:
            print(f"DEBUG processing {file_path}, found execution_time: {execution_time}")

        # Extract performance data from perf.json (method level)
        perf_data = extract_performance_data(file_path)
        if debug_this_file:
            print(f"DEBUG processing {file_path}, found perf_data: {perf_data}")

        # Extract metric-level performance data from perf.json
        metric_perf_data = extract_metric_performance_data(file_path)
        if debug_this_file:
            print(f"DEBUG processing {file_path}, found metric_perf_data: {metric_perf_data}")

        # Ensure seed is int if not None
        if seed is not None and not isinstance(seed, int):
            try:
                seed = int(float(seed))
            except (ValueError, TypeError):
                seed = None

        # Create base method result (one per method run, regardless of metrics)
        method_result = {
            'source_dir': os.path.basename(source_dir),
            'backend': backend,
            'run_timestamp': timestamp,
            'dataset_generator': dataset_gen,
            'dataset_name': dataset_name,
            'true_k': true_k,
            'has_noise': has_noise,
            'method': method,
            'seed': seed,
            'execution_time_seconds': execution_time,
        }
        # Add performance data
        method_result.update(perf_data)

        # Read the gzipped CSV file for metric data
        with gzip.open(file_path, 'rt') as f:
            content = f.read()
            if not content.strip():
                print(f"Warning: Empty file encountered: {file_path}")
                metric_result = {
                    'source_dir': os.path.basename(source_dir),
                    'backend': backend,
                    'run_timestamp': timestamp,
                    'dataset_generator': dataset_gen,
                    'dataset_name': dataset_name,
                    'true_k': true_k,
                    'has_noise': has_noise,
                    'method': method,
                    'seed': seed,
                    'metric': metric,
                    'score': None,
                    'runtime': metric_perf_data.get('runtime'),
                    'duplicate_k_anomaly': False,
                    'empty_file': True,
                    'missing_true_k_score': False
                }
                return method_result, metric_result

            # Reset file pointer to beginning
            f.seek(0)

            # Read the header and data
            reader = csv.reader(f)
            try:
                header = next(reader)
                data_rows = list(reader)
            except StopIteration:
                print(f"Warning: CSV file has no header or data: {file_path}")
                metric_result = {
                    'source_dir': os.path.basename(source_dir),
                    'backend': backend,
                    'run_timestamp': timestamp,
                    'dataset_generator': dataset_gen,
                    'dataset_name': dataset_name,
                    'true_k': true_k,
                    'has_noise': has_noise,
                    'method': method,
                    'seed': seed,
                    'metric': metric,
                    'score': None,
                    'runtime': metric_perf_data.get('runtime'),
                    'duplicate_k_anomaly': False,
                    'empty_file': True,
                    'missing_true_k_score': False
                }
                return method_result, metric_result

            if len(data_rows) == 0:
                metric_result = {
                    'source_dir': os.path.basename(source_dir),
                    'backend': backend,
                    'run_timestamp': timestamp,
                    'dataset_generator': dataset_gen,
                    'dataset_name': dataset_name,
                    'true_k': true_k,
                    'has_noise': has_noise,
                    'method': method,
                    'seed': seed,
                    'metric': metric,
                    'score': None,
                    'runtime': metric_perf_data.get('runtime'),
                    'duplicate_k_anomaly': False,
                    'empty_file': True,
                    'missing_true_k_score': False
                }
                return method_result, metric_result

            # Process the data
            data = data_rows[0]  # Assuming single row of values

            # Check for duplicate k values with different results
            duplicate_k_anomaly = False
            k_values = {}
            for i, k in enumerate(header):
                if i < len(data):
                    k_cleaned = k.strip('"')
                    try:
                        value = float(data[i])
                        if k_cleaned in k_values:
                            if abs(k_values[k_cleaned] - value) > 1e-3:
                                duplicate_k_anomaly = True
                        else:
                            k_values[k_cleaned] = value
                    except (ValueError, TypeError):
                        if k_cleaned not in k_values:
                            k_values[k_cleaned] = data[i]

            # Create metric result
            metric_result = {
                'source_dir': os.path.basename(source_dir),
                'backend': backend,
                'run_timestamp': timestamp,
                'dataset_generator': dataset_gen,
                'dataset_name': dataset_name,
                'true_k': true_k,
                'has_noise': has_noise,
                'method': method,
                'seed': seed,
                'metric': metric,
                'score': None,
                'runtime': metric_perf_data.get('runtime'),
                'duplicate_k_anomaly': duplicate_k_anomaly,
                'empty_file': False,
                'missing_true_k_score': False
            }

            # Add k values from header and corresponding scores
            for i, k in enumerate(header):
                if i < len(data):
                    k_cleaned = k.strip('"')
                    try:
                        metric_result[k_cleaned] = float(data[i])
                    except ValueError:
                        metric_result[k_cleaned] = data[i]

            # Extract score for k=true_k and add as 'score' column
            true_k_col = f'k={true_k}' if true_k is not None else None
            if true_k_col and true_k_col in metric_result:
                metric_result['score'] = metric_result[true_k_col]
                metric_result['missing_true_k_score'] = False
            else:
                metric_result['score'] = None
                metric_result['missing_true_k_score'] = True if true_k is not None else False

            return method_result, metric_result

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error processing {file_path}: {e}")
        print(f"Full error details:\n{error_details}")
        return None, None


def main():
    """
    Main function to aggregate all clustbench.scores.gz files into two denormalized datasets:
    1. method-performance.csv: Dataset x Method x Seed level (execution metrics)
    2. metric-performance.csv: Dataset x Method x Seed x Metric level (clustering scores)
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Aggregate clustbench scores into method and metric performance datasets',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('root_dirs', nargs='+', help='Root directories containing clustbench output')
    parser.add_argument('--out_dir', type=str, default='.', help='Output directory for aggregated results')
    parser.add_argument('--debug', '-d', action='store_true',
                        help='Enable debug output')
    parser.add_argument('--format', '-f', type=str, choices=['csv', 'parquet', 'both'], default='csv',
                        help='Output format: csv, parquet, or both (default: csv)')
    parser.add_argument('--cores', '-c', type=int, default=multiprocessing.cpu_count(),
                        help='Number of CPU cores to use for parallel processing')
    args = parser.parse_args()

    all_method_results = []
    all_metric_results = []
    all_backends = []
    all_timestamps = []
    all_duplicate_k_anomaly_files = []
    source_dirs = []

    # Process directories
    if args.cores > 1 and len(args.root_dirs) > 1:
        print(f"Using {args.cores} CPU cores for parallel processing...")
        with multiprocessing.Pool(processes=args.cores) as pool:
            process_results = pool.map(partial(process_dir, debug_mode=args.debug), args.root_dirs)

            for result_tuple in process_results:
                method_results, metric_results, backend, timestamp, duplicate_k_anomaly_files, dir_name = result_tuple
                if method_results:
                    all_method_results.extend(method_results)
                if metric_results:
                    all_metric_results.extend(metric_results)
                if backend and backend not in all_backends:
                    all_backends.append(backend)
                if timestamp and timestamp not in all_timestamps:
                    all_timestamps.append(timestamp)
                all_duplicate_k_anomaly_files.extend(duplicate_k_anomaly_files)
                source_dirs.append(dir_name)
    else:
        if args.cores == 1:
            print("Using single-core processing...")
        for base_dir in args.root_dirs:
            method_results, metric_results, backend, timestamp, duplicate_k_anomaly_files, dir_name = process_run(base_dir, args.debug)
            if method_results:
                all_method_results.extend(method_results)
            if metric_results:
                all_metric_results.extend(metric_results)
            if backend and backend not in all_backends:
                all_backends.append(backend)
            if timestamp and timestamp not in all_timestamps:
                all_timestamps.append(timestamp)
            all_duplicate_k_anomaly_files.extend(duplicate_k_anomaly_files)
            source_dirs.append(dir_name)

    # Create output directory if it doesn't exist
    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)

    # Generate base filename
    if len(source_dirs) == 1:
        dir_name = source_dirs[0]
        backend = all_backends[0] if all_backends else None
        timestamp = all_timestamps[0] if all_timestamps else None

        # Build filename components, filtering out None values
        name_parts = [dir_name]
        if backend:
            name_parts.append(backend)
        if timestamp:
            name_parts.append(timestamp)
        else:
            # If no timestamp available, add current timestamp
            current_timestamp = pd.Timestamp.now().strftime('%Y%m%d%H%M')
            name_parts.append(current_timestamp)
        base_name = '_'.join(name_parts)
    else:
        current_timestamp = pd.Timestamp.now().strftime('%Y%m%d%H%M')
        backends_str = '_'.join(all_backends) if len(all_backends) <= 3 else f'{len(all_backends)}_backends'
        base_name = f'multi_{len(source_dirs)}_dirs_{backends_str}_{current_timestamp}'

    # Process method results
    if all_method_results:
        # Remove duplicates (same method run may appear multiple times due to multiple metrics)
        method_df_data = {}
        for result in all_method_results:
            key = (result['source_dir'], result['backend'], result['run_timestamp'],
                   result['dataset_generator'], result['dataset_name'], result['method'], result['seed'])
            if key not in method_df_data:
                method_df_data[key] = result

        method_df = pd.DataFrame(list(method_df_data.values()))

        # Organize columns
        method_meta_columns = ['source_dir', 'backend', 'run_timestamp', 'dataset_generator',
                              'dataset_name', 'true_k', 'has_noise', 'method', 'seed',
                              'execution_time_seconds', 'runtime', 'threads', 'disk_read',
                              'disk_write', 'avg_load', 'peak_rss']
        method_df = method_df[method_meta_columns]

        # Output method performance
        if args.format in ['csv', 'both']:
            method_csv_file = os.path.join(args.out_dir, f"method-performance_{base_name}.csv")
            method_df.to_csv(method_csv_file, index=False)
            print(f"Method performance data saved to {method_csv_file}")

        if args.format in ['parquet', 'both']:
            method_parquet_file = os.path.join(args.out_dir, f"method-performance_{base_name}.parquet")
            method_df.to_parquet(method_parquet_file, index=False)
            print(f"Method performance parquet saved to {method_parquet_file}")

        print(f"Method performance dataset: {len(method_df)} records")

    # Process metric results
    if all_metric_results:
        metric_df = pd.DataFrame(all_metric_results)

        # Organize columns - metadata first, then k values
        all_columns = metric_df.columns.tolist()
        metric_meta_columns = ['source_dir', 'backend', 'run_timestamp', 'dataset_generator',
                              'dataset_name', 'true_k', 'has_noise', 'method', 'seed', 'metric', 'score',
                              'runtime', 'duplicate_k_anomaly', 'empty_file', 'missing_true_k_score']
        k_columns = [col for col in all_columns if col not in metric_meta_columns]

        # Sort k columns numerically
        def extract_k(col):
            if col.startswith('k='):
                try:
                    return int(col.split('=')[1])
                except:
                    return 999999
            return 999999

        k_columns.sort(key=extract_k)

        # Reorder columns
        metric_df = metric_df[metric_meta_columns + k_columns]

        # Output metric performance
        if args.format in ['csv', 'both']:
            metric_csv_file = os.path.join(args.out_dir, f"metric-performance_{base_name}.csv")
            metric_df.to_csv(metric_csv_file, index=False)
            print(f"Metric performance data saved to {metric_csv_file}")

        if args.format in ['parquet', 'both']:
            metric_parquet_file = os.path.join(args.out_dir, f"metric-performance_{base_name}.parquet")
            metric_df.to_parquet(metric_parquet_file, index=False)
            print(f"Metric performance parquet saved to {metric_parquet_file}")

        print(f"Metric performance dataset: {len(metric_df)} records")

        # Report statistics
        empty_file_count = metric_df['empty_file'].sum()
        missing_true_k_score_count = metric_df['missing_true_k_score'].sum()
        duplicate_k_anomaly_count = metric_df['duplicate_k_anomaly'].sum()

        if empty_file_count > 0:
            print(f"Warning: Found {empty_file_count} empty files out of {len(metric_df)} total records")

        if missing_true_k_score_count > 0:
            print(f"Warning: Found {missing_true_k_score_count} records with missing true_k score values")

        if duplicate_k_anomaly_count > 0:
            print(f"Warning: Found {duplicate_k_anomaly_count} records with duplicate k anomalies")

        if args.debug and all_duplicate_k_anomaly_files:
            print("\nFiles with Duplicate K Anomaly:")
            for file in all_duplicate_k_anomaly_files:
                print(f"- {file}")

    else:
        print("No matching score files found. Nothing to do.")


if __name__ == "__main__":
    main()
