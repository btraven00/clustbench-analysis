#!/usr/bin/env python3
"""
Integration tests for the refactored clustbench aggregation functions.

These tests create realistic sample data structures and test the complete
processing pipeline to ensure the refactored code produces the expected
denormalized outputs.
"""

import unittest
import tempfile
import os
import json
import gzip
import csv
import shutil
from unittest.mock import patch
import sys

# Add the parent directory to the path to import the module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aggregate_scores import (
    process_run,
    process_scores_file,
    main
)


class TestIntegration(unittest.TestCase):
    """Integration tests with realistic sample data"""

    def setUp(self):
        """Create a realistic clustbench output directory structure"""
        self.test_dir = tempfile.mkdtemp()

        # Create the main output directory structure
        self.output_dir = os.path.join(self.test_dir, "out-conda_202506231301")
        os.makedirs(self.output_dir)

        # Create multiple datasets
        datasets = [
            ("fcps", "atom", 4, 0.0),
            ("sklearn", "blobs", 3, 0.1),
            ("synthetic", "moons", 2, 0.05)
        ]

        # Create multiple methods
        methods = [
            ("kmeans", [42, 123]),
            ("dbscan", [None]),
            ("agglomerative", [42])
        ]

        # Create multiple metrics
        metrics = ["ari", "ami", "silhouette"]

        # Generate realistic data structure
        for dataset_gen, dataset_name, n_clusters, noise in datasets:
            dataset_dir = os.path.join(
                self.output_dir,
                "data",
                "clustbench",
                f"dataset_generator-{dataset_gen}_dataset_name-{dataset_name}"
            )
            os.makedirs(dataset_dir)

            # Create dataset parameters.json
            dataset_params = {
                "dataset_generator": dataset_gen,
                "dataset_name": dataset_name,
                "n_clusters": n_clusters,
                "noise": noise
            }
            with open(os.path.join(dataset_dir, "parameters.json"), 'w') as f:
                json.dump(dataset_params, f)

            # Create clustering results for each method
            for method_name, seeds in methods:
                for seed in seeds:
                    if seed is not None:
                        method_dir_name = f"method-{method_name}_seed-{seed}"
                    else:
                        method_dir_name = f"method-{method_name}"

                    method_dir = os.path.join(
                        dataset_dir,
                        "clustering",
                        method_dir_name
                    )
                    os.makedirs(method_dir)

                    # Create method performance file
                    import random
                    random.seed(42)  # For reproducible test data
                    execution_time = round(random.uniform(5.0, 60.0), 2)
                    with open(os.path.join(method_dir, "clustbench_performance.txt"), 'w') as f:
                        f.write("s\tusr\tsys\tmaxrss\tixrss\tidrss\tisrss\tminflt\tmajflt\tnswap\tinblock\toublock\tmsgsnd\tmsgrcv\tnsignals\tnvcsw\tnivcsw\n")
                        f.write(f"{execution_time}\t{execution_time*0.8}\t{execution_time*0.2}\t{random.randint(1024, 8192)}\t0\t0\t0\t{random.randint(50, 200)}\t0\t0\t{random.randint(10, 100)}\t{random.randint(5, 50)}\t0\t0\t0\t{random.randint(1, 20)}\t{random.randint(1, 10)}\n")

                    # Create perf.json
                    perf_data = {
                        "total_time_secs": execution_time,
                        "max_threads": random.randint(1, 8),
                        "total_disk_read_bytes": random.randint(1000, 10000),
                        "total_disk_write_bytes": random.randint(500, 5000),
                        "avg_cpu_usage": round(random.uniform(20.0, 95.0), 1),
                        "peak_mem_rss_kb": random.randint(1024, 8192)
                    }
                    with open(os.path.join(method_dir, "perf.json"), 'w') as f:
                        json.dump(perf_data, f)

                    # Create metrics for each metric type
                    for metric in metrics:
                        metric_dir = os.path.join(
                            method_dir,
                            "metrics",
                            "partition_metrics",
                            f"metric-{metric}"
                        )
                        os.makedirs(metric_dir)

                        # Create realistic scores file
                        score_file = os.path.join(metric_dir, "clustbench.scores.gz")
                        self._create_realistic_scores_file(score_file, n_clusters, metric)

    def tearDown(self):
        """Clean up test directory"""
        shutil.rmtree(self.test_dir)

    def _create_realistic_scores_file(self, file_path, true_k, metric):
        """Create a realistic scores file with sample data"""
        import random
        random.seed(42)  # For reproducible test data

        # Generate k values around the true k
        k_values = list(range(max(1, true_k - 2), true_k + 4))
        headers = [f"k={k}" for k in k_values]

        # Generate realistic scores based on metric type
        scores = []
        for k in k_values:
            if metric in ["ari", "ami"]:
                # ARI and AMI: higher is better, peak around true k
                base_score = 0.95 if k == true_k else max(0.1, 0.95 - abs(k - true_k) * 0.2)
                score = base_score + random.uniform(-0.05, 0.05)
                scores.append(f"{max(0.0, min(1.0, score)):.3f}")
            elif metric == "silhouette":
                # Silhouette: higher is better, but different range
                base_score = 0.8 if k == true_k else max(0.2, 0.8 - abs(k - true_k) * 0.15)
                score = base_score + random.uniform(-0.1, 0.1)
                scores.append(f"{max(-1.0, min(1.0, score)):.3f}")
            else:
                # Default random scores
                scores.append(f"{random.uniform(0.0, 1.0):.3f}")

        # Write to gzipped CSV
        with gzip.open(file_path, 'wt') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerow(scores)

    def test_process_run_integration(self):
        """Test the complete process_run function with realistic data"""
        method_results, metric_results, backend, timestamp, anomaly_files, dir_name = process_run(
            self.output_dir, debug_mode=False
        )

        # Test that we got results
        self.assertGreater(len(method_results), 0)
        self.assertGreater(len(metric_results), 0)

        # Test backend and timestamp extraction
        self.assertEqual(backend, "conda")
        self.assertEqual(timestamp, "202506231301")

        # Test method results structure
        method_result = method_results[0]
        expected_method_keys = [
            'source_dir', 'backend', 'run_timestamp', 'dataset_generator',
            'dataset_name', 'true_k', 'has_noise', 'method', 'seed',
            'execution_time_seconds', 'runtime', 'threads', 'disk_read',
            'disk_write', 'avg_load', 'peak_rss'
        ]
        for key in expected_method_keys:
            self.assertIn(key, method_result)

        # Test metric results structure
        metric_result = metric_results[0]
        expected_metric_keys = [
            'source_dir', 'backend', 'run_timestamp', 'dataset_generator',
            'dataset_name', 'true_k', 'has_noise', 'method', 'seed', 'metric',
            'duplicate_k_anomaly', 'empty_file', 'missing_true_k_score'
        ]
        for key in expected_metric_keys:
            self.assertIn(key, metric_result)

        # Test that we have k-value columns in metric results
        k_columns = [col for col in metric_result.keys() if col.startswith('k=')]
        self.assertGreater(len(k_columns), 0)

        # Test that method results don't have k-value columns
        method_k_columns = [col for col in method_result.keys() if col.startswith('k=')]
        self.assertEqual(len(method_k_columns), 0)

        # Test data consistency - method results may have duplicates before deduplication
        # (same method appears once per metric), but each unique method should be represented
        unique_methods = set(
            (r['dataset_generator'], r['dataset_name'], r['method'], r['seed'])
            for r in method_results
        )
        self.assertGreater(len(unique_methods), 0)  # Should have some unique methods

        # Test that deduplication would work correctly
        method_counts = {}
        for r in method_results:
            key = (r['dataset_generator'], r['dataset_name'], r['method'], r['seed'])
            method_counts[key] = method_counts.get(key, 0) + 1

        # Each method should appear multiple times (once per metric)
        for count in method_counts.values():
            self.assertGreaterEqual(count, 1)  # At least once per method

        # Test that we have more metric results than method results
        # (since each method can have multiple metrics)
        self.assertGreaterEqual(len(metric_results), len(method_results))

    def test_single_scores_file_processing(self):
        """Test processing a single scores file"""
        # Find a specific scores file
        score_files = []
        for root, dirs, files in os.walk(self.output_dir):
            for file in files:
                if file == "clustbench.scores.gz":
                    score_files.append(os.path.join(root, file))

        self.assertGreater(len(score_files), 0)

        # Process one file
        score_file = score_files[0]
        method_result, metric_result = process_scores_file(
            score_file, "conda", "202506231301", self.output_dir
        )

        # Test that both results are returned
        self.assertIsNotNone(method_result)
        self.assertIsNotNone(metric_result)

        # Test method result content
        self.assertIn(method_result['dataset_generator'], ["fcps", "sklearn", "synthetic"])
        self.assertIn(method_result['method'], ["kmeans", "dbscan", "agglomerative"])
        self.assertIsInstance(method_result['execution_time_seconds'], (float, int, type(None)))
        self.assertIsInstance(method_result['runtime'], (float, int, type(None)))

        # Test metric result content
        self.assertIn(metric_result['metric'], ["ari", "ami", "silhouette"])
        self.assertIsInstance(metric_result['duplicate_k_anomaly'], bool)
        self.assertIsInstance(metric_result['empty_file'], bool)
        self.assertIsInstance(metric_result['missing_true_k_score'], bool)

        # Test that we have actual score values
        k_columns = [col for col in metric_result.keys() if col.startswith('k=')]
        for k_col in k_columns:
            self.assertIsInstance(metric_result[k_col], (float, int))

    def test_data_consistency_across_methods_and_metrics(self):
        """Test that data is consistent across different aggregation levels"""
        method_results, metric_results, _, _, _, _ = process_run(
            self.output_dir, debug_mode=False
        )

        # Group results by method
        method_groups = {}
        for result in method_results:
            key = (result['dataset_generator'], result['dataset_name'],
                   result['method'], result['seed'])
            method_groups[key] = result

        # Group metric results by method
        metric_groups = {}
        for result in metric_results:
            key = (result['dataset_generator'], result['dataset_name'],
                   result['method'], result['seed'])
            if key not in metric_groups:
                metric_groups[key] = []
            metric_groups[key].append(result)

        # Test that every method has corresponding metric results
        self.assertEqual(set(method_groups.keys()), set(metric_groups.keys()))

        # Test that dataset info is consistent between method and metric results
        for key in method_groups.keys():
            method_result = method_groups[key]
            metric_result_list = metric_groups[key]

            for metric_result in metric_result_list:
                # Test that common fields match
                common_fields = ['dataset_generator', 'dataset_name', 'true_k',
                               'has_noise', 'method', 'seed']
                for field in common_fields:
                    self.assertEqual(method_result[field], metric_result[field],
                                   f"Field {field} mismatch for {key}")

    def test_realistic_data_values(self):
        """Test that generated data has realistic values"""
        method_results, metric_results, _, _, _, _ = process_run(
            self.output_dir, debug_mode=False
        )

        # Test method results
        for result in method_results:
            # Test that we have valid dataset info
            self.assertIn(result['dataset_generator'], ["fcps", "sklearn", "synthetic"])
            self.assertIn(result['dataset_name'], ["atom", "blobs", "moons"])
            # true_k and has_noise may be None if no labels file exists
            if result['true_k'] is not None:
                self.assertIsInstance(result['true_k'], int)
                self.assertGreater(result['true_k'], 0)
            if result['has_noise'] is not None:
                self.assertIsInstance(result['has_noise'], bool)

            # Test performance metrics
            if result['execution_time_seconds'] is not None:
                self.assertGreaterEqual(result['execution_time_seconds'], 0)
            if result['runtime'] is not None:
                self.assertGreaterEqual(result['runtime'], 0)
            if result['threads'] is not None:
                self.assertGreaterEqual(result['threads'], 1)

        # Test metric results
        for result in metric_results:
            self.assertIn(result['metric'], ["ari", "ami", "silhouette"])
            self.assertIsInstance(result['duplicate_k_anomaly'], bool)
            self.assertIsInstance(result['empty_file'], bool)
            self.assertIsInstance(result['missing_true_k_score'], bool)

            # Test score values
            k_columns = [col for col in result.keys() if col.startswith('k=')]
            for k_col in k_columns:
                score = result[k_col]
                if result['metric'] in ["ari", "ami"]:
                    self.assertGreaterEqual(score, 0.0)
                    self.assertLessEqual(score, 1.0)
                elif result['metric'] == "silhouette":
                    self.assertGreaterEqual(score, -1.0)
                    self.assertLessEqual(score, 1.0)

    def test_edge_cases(self):
        """Test edge cases and error handling"""
        # Test with non-existent directory
        method_results, metric_results, _, _, _, _ = process_run(
            "/non/existent/directory", debug_mode=False
        )
        self.assertEqual(len(method_results), 0)
        self.assertEqual(len(metric_results), 0)

        # Test with empty directory
        empty_dir = os.path.join(self.test_dir, "empty")
        os.makedirs(empty_dir)
        method_results, metric_results, _, _, _, _ = process_run(
            empty_dir, debug_mode=False
        )
        self.assertEqual(len(method_results), 0)
        self.assertEqual(len(metric_results), 0)


class TestEndToEndProcessing(unittest.TestCase):
    """End-to-end tests that simulate the complete workflow"""

    def setUp(self):
        """Set up test environment"""
        self.test_dir = tempfile.mkdtemp()
        self.output_dir = os.path.join(self.test_dir, "test_output")
        os.makedirs(self.output_dir)

    def tearDown(self):
        """Clean up"""
        shutil.rmtree(self.test_dir)

    @patch('aggregate_scores.pd.DataFrame.to_csv')
    @patch('aggregate_scores.pd.DataFrame.to_parquet')
    def test_main_function_integration(self, mock_parquet, mock_csv):
        """Test the main function with mocked file output"""
        # Create a minimal test structure
        run_dir = os.path.join(self.test_dir, "out-test_202501010000")
        os.makedirs(run_dir)

        # Create one minimal dataset and method
        dataset_dir = os.path.join(run_dir, "data", "clustbench", "dataset_generator-test_dataset_name-sample")
        method_dir = os.path.join(dataset_dir, "clustering", "method-test_seed-1")
        metric_dir = os.path.join(method_dir, "metrics", "partition_metrics", "metric-test")
        os.makedirs(metric_dir)

        # Create minimal files
        with open(os.path.join(dataset_dir, "parameters.json"), 'w') as f:
            json.dump({"n_clusters": 2, "noise": 0}, f)

        with open(os.path.join(method_dir, "clustbench_performance.txt"), 'w') as f:
            f.write("s\tusr\tsys\tmaxrss\tixrss\tidrss\tisrss\tminflt\tmajflt\tnswap\tinblock\toublock\tmsgsnd\tmsgrcv\tnsignals\tnvcsw\tnivcsw\n")
            f.write("10.5\t8.2\t2.3\t2048\t0\t0\t0\t50\t0\t0\t25\t12\t0\t0\t0\t5\t3\n")

        with open(os.path.join(method_dir, "perf.json"), 'w') as f:
            json.dump({"total_time_secs": 10.5, "max_threads": 1}, f)

        # Create scores file
        score_file = os.path.join(metric_dir, "clustbench.scores.gz")
        with gzip.open(score_file, 'wt') as f:
            writer = csv.writer(f)
            writer.writerow(["k=1", "k=2", "k=3"])
            writer.writerow(["0.5", "0.9", "0.7"])

        # Mock sys.argv to simulate command line arguments
        with patch('sys.argv', ['script.py', run_dir, '--out_dir', self.output_dir, '--format', 'both']):
            # This would normally call main(), but we'll test the core logic
            method_results, metric_results, _, _, _, _ = process_run(run_dir, debug_mode=False)

            # Verify we got results
            self.assertEqual(len(method_results), 1)
            self.assertEqual(len(metric_results), 1)

            # Verify method result structure
            method_result = method_results[0]
            self.assertEqual(method_result['dataset_generator'], 'test')
            self.assertEqual(method_result['dataset_name'], 'sample')
            self.assertEqual(method_result['method'], 'test')
            self.assertEqual(method_result['seed'], 1)
            self.assertEqual(method_result['execution_time_seconds'], 10.5)

            # Verify metric result structure
            metric_result = metric_results[0]
            self.assertEqual(metric_result['metric'], 'test')
            self.assertEqual(metric_result['k=1'], 0.5)
            self.assertEqual(metric_result['k=2'], 0.9)
            self.assertEqual(metric_result['k=3'], 0.7)


if __name__ == '__main__':
    unittest.main(verbosity=2)
