#!/usr/bin/env python3
"""
Script to aggregate clustbench.scores.gz files from the output tree,
extracting dataset generator, dataset name, method, metric, scores,
performance time (seconds), true k value, noise presence, and anomalies in k values.
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
    """Process a clustbench.scores.gz file and extract relevant data"""
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

def get_dataset_true_k_and_noise(base_dir, dataset_gen, dataset_name):
    """
    Extract the true number of clusters and noise presence from dataset labels file.
    
    Parameters:
    base_dir (str): Base directory for clustbench output
    dataset_gen (str): Dataset generator name
    dataset_name (str): Dataset name
    
    Returns:
    tuple: (true_k, has_noise) where true_k is the number of unique non-zero labels,
           and has_noise is True if label 0 is present
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
    """Main function to aggregate all clustbench.scores.gz files"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Aggregate clustbench scores from output directory')
    parser.add_argument('--root', '-r', type=str, default='out_apptainer-202505301205',
                        help='Root directory containing clustbench output')
    parser.add_argument('--debug', '-d', action='store_true',
                        help='Enable debug output')
    args = parser.parse_args()
    
    # Base directory
    base_dir = args.root
    debug_mode = args.debug
    
    # Extract backend and timestamp
    backend, timestamp = extract_backend_timestamp(base_dir)
    print(f"Detected backend: {backend}, timestamp: {timestamp}")
    
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
                true_k, has_noise = get_dataset_true_k_and_noise(base_dir, dataset_gen, dataset_name)
                dataset_info_cache[cache_key] = (true_k, has_noise)
                if debug_mode:
                    print(f"Dataset {dataset_gen}_{dataset_name}: true_k={true_k}, has_noise={has_noise}")
            else:
                true_k, has_noise = dataset_info_cache[cache_key]
            
            # Add true_k and has_noise to the result
            result['true_k'] = true_k
            result['has_noise'] = has_noise
            
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
    
    # Convert to DataFrame
    if results:
        df = pd.DataFrame(results)
        
        # Add backend and timestamp columns
        backend, timestamp = extract_backend_timestamp(base_dir)
        df['backend'] = backend
        df['run_timestamp'] = timestamp
        
        # Organize columns - metadata first, then k values
        all_columns = df.columns.tolist()
        meta_columns = ['backend', 'run_timestamp', 'dataset_generator', 'dataset_name', 'true_k', 'has_noise', 'method', 'metric', 'score', 'execution_time_seconds', 'duplicate_k_anomaly']
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
        
        # Save to CSV
        output_file = f'clustbench_aggregated_scores_{backend}_{timestamp}.csv' if backend and timestamp else 'clustbench_aggregated_scores.csv'
        df.to_csv(output_file, index=False)
        print(f"Aggregated scores saved to {output_file}")
        
        # Show a summary
        print(f"\nProcessed {len(results)} out of {len(score_files)} score files")
        print(f"Dataset generators: {df['dataset_generator'].unique()}")
        print(f"Unique datasets: {df['dataset_name'].nunique()}")
        print(f"Unique methods: {df['method'].nunique()}")
        print(f"Unique metrics: {df['metric'].nunique()}")
        
        # Check for and report k value anomalies
        # Convert boolean columns to numeric for counting
        df['duplicate_k_anomaly'] = df['duplicate_k_anomaly'].astype(int)
        
        duplicate_anomaly_count = df['duplicate_k_anomaly'].sum()
        
        if duplicate_anomaly_count > 0:
            print(f"\n⚠️ WARNING: Detected {duplicate_anomaly_count} rows with duplicate k anomalies!")
            print("These are cases where the same k value appears multiple times in the header")
            print("with significantly different scores (difference > 1E-3).")
                
            # Report duplicate k anomalies
            if duplicate_anomaly_count > 0:
                duplicate_anomaly_df = df[df['duplicate_k_anomaly'] == 1].copy()
                duplicate_anomaly_df['method_display'] = duplicate_anomaly_df['method'].fillna("NO_METHOD")
                duplicate_groups = duplicate_anomaly_df.groupby(['dataset_generator', 'dataset_name', 'method_display', 'metric']).size()
                print("\nDuplicate k anomalies by (dataset_generator, dataset_name, method, metric):")
                for index, count in duplicate_groups.items():
                    gen, name, method, metric = index
                    method_display = "NO_METHOD" if pd.isna(method) or method == "" else method
                    print(f"  - {gen}, {name}, {method_display}, {metric}")
                
                if debug_mode:
                    print("\nFiles with duplicate k anomalies:")
                    for i, file_path in enumerate(duplicate_k_anomaly_files):
                        print(f"  {i+1}. {Path(file_path).relative_to(Path(base_dir))}")
                # Detailed anomaly information is handled in the files with duplicate k anomalies section above
        print("\nExample rows:")
        print(df.head())
    else:
        print("No results found")

if __name__ == "__main__":
    main()