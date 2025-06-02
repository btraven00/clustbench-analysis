#!/usr/bin/env python3
"""
Script to aggregate clustbench.scores.gz files from the output tree,
extracting dataset generator, dataset name, method, metric, scores,
and performance time (seconds).
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
    
    # Try to get from parameters.json
    params_file = os.path.join(os.path.dirname(path), 'parameters.json')
    if os.path.exists(params_file):
        with open(params_file, 'r') as f:
            try:
                params = json.load(f)
                return params.get('method', '')
            except:
                pass
    
    # Go up one level and try again
    parent_dir = os.path.dirname(os.path.dirname(path))
    if parent_dir == path:  # Stop recursion at root
        return ''
    return extract_method_info(parent_dir)

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
            
            # Create result dictionary
            result = {
                'dataset_generator': dataset_gen,
                'dataset_name': dataset_name,
                'method': method,
                'metric': metric,
                'execution_time_seconds': execution_time
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

def main():
    """Main function to aggregate all clustbench.scores.gz files"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Aggregate clustbench scores from output directory')
    parser.add_argument('--root', '-r', type=str, default='out_apptainer-202505301205',
                        help='Root directory containing clustbench output')
    args = parser.parse_args()
    
    # Base directory
    base_dir = args.root
    
    # Extract backend and timestamp
    backend, timestamp = extract_backend_timestamp(base_dir)
    print(f"Detected backend: {backend}, timestamp: {timestamp}")
    
    # Find all clustbench.scores.gz files
    score_files = glob.glob(f'{base_dir}/**/clustbench.scores.gz', recursive=True)
    print(f"Found {len(score_files)} score files")
    
    # Process each file and collect results
    results = []
    for i, file_path in enumerate(score_files):
        if i % 100 == 0 and i > 0:
            print(f"Processed {i}/{len(score_files)} files...")
        result = process_scores_file(file_path)
        if result:
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
        meta_columns = ['backend', 'run_timestamp', 'dataset_generator', 'dataset_name', 'method', 'metric', 'execution_time_seconds']
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
        print("\nExample rows:")
        print(df.head())
    else:
        print("No results found")

if __name__ == "__main__":
    main()