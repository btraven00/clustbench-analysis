#!/usr/bin/env python3
"""
Unit tests for the clustbench aggregation functions.

Streamlined test suite focusing on core functionality and integration tests.
"""

import unittest
import tempfile
import os
import json
import gzip
import csv
from unittest.mock import patch, mock_open
import sys

# Add the parent directory to the path to import the module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aggregate_scores import (
    extract_dataset_info,
    extract_method_info,
    extract_metric_info,
    extract_performance_data,
    find_method_performance,
    extract_dataset_true_k_and_noise,
    extract_backend_timestamp,
    process_scores_file
)


class TestExtractionFunctions(unittest.TestCase):
    """Test core extraction functions with key scenarios"""

    def test_dataset_info_extraction(self):
        """Test dataset info extraction from path"""
        path = "/data/clustbench/dataset_generator-fcps_dataset_name-atom/clustering/method-test"
        gen, name = extract_dataset_info(path)
        self.assertEqual(gen, "fcps")
        self.assertEqual(name, "atom")

    def test_method_info_extraction_with_seed(self):
        """Test method info extraction with seed"""
        path = "/data/method-kmeans_seed-123/metrics/partition_metrics/metric-ari/clustbench.scores.gz"
        result = extract_method_info(path)
        self.assertEqual(result['method'], "kmeans")
        self.assertEqual(result['seed'], 123)

    def test_method_info_extraction_linkage(self):
        """Test linkage method extraction"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create directory structure for linkage method
            linkage_dir = os.path.join(tmpdir, "agglomerative", "linkage-ward")
            os.makedirs(linkage_dir)

            path = os.path.join(linkage_dir, "metrics", "partition_metrics", "metric-ari", "clustbench.scores.gz")
            result = extract_method_info(path)
            self.assertEqual(result['method'], "agglomerative_linkage-ward")

    def test_metric_info_extraction(self):
        """Test metric info extraction from path"""
        path = "/data/method-test/metrics/partition_metrics/metric-ari/clustbench.scores.gz"
        metric = extract_metric_info(path)
        self.assertEqual(metric, "ari")

    def test_backend_timestamp_extraction(self):
        """Test backend and timestamp extraction"""
        backend, timestamp = extract_backend_timestamp("out_conda-202506231301")
        self.assertEqual(backend, "conda")
        self.assertEqual(timestamp, "202506231301")


class TestPerformanceExtraction(unittest.TestCase):
    """Test performance data extraction"""

    def test_find_method_performance_success(self):
        """Test successful extraction of method execution time"""
        with tempfile.TemporaryDirectory() as tmpdir:
            method_dir = os.path.join(tmpdir, "method-test")
            os.makedirs(method_dir)

            # Create clustbench_performance.txt file
            perf_file = os.path.join(method_dir, "clustbench_performance.txt")
            with open(perf_file, 'w') as f:
                f.write("s\tusr\tsys\tmaxrss\tixrss\tidrss\tisrss\tminflt\tmajflt\tnswap\tinblock\toublock\tmsgsnd\tmsgrcv\tnsignals\tnvcsw\tnivcsw\n")
                f.write("45.67\t35.2\t10.47\t8192\t0\t0\t0\t200\t0\t0\t100\t50\t0\t0\t0\t20\t10\n")

            scores_dir = os.path.join(method_dir, "metrics", "partition_metrics", "metric-ari")
            os.makedirs(scores_dir)
            score_file = os.path.join(scores_dir, "clustbench.scores.gz")

            result = find_method_performance(score_file)
            self.assertEqual(result, 45.67)

    def test_find_method_performance_missing_file(self):
        """Test when performance file is missing"""
        with tempfile.TemporaryDirectory() as tmpdir:
            scores_dir = os.path.join(tmpdir, "method-test", "metrics", "partition_metrics", "metric-ari")
            os.makedirs(scores_dir)
            score_file = os.path.join(scores_dir, "clustbench.scores.gz")

            result = find_method_performance(score_file)
            self.assertIsNone(result)

    def test_extract_performance_data_success(self):
        """Test successful extraction of performance data"""
        with tempfile.TemporaryDirectory() as tmpdir:
            method_dir = os.path.join(tmpdir, "method-test")
            os.makedirs(method_dir)

            perf_data = {
                "total_time_secs": 30.5,
                "max_threads": 4,
                "total_disk_read_bytes": 2048,
                "total_disk_write_bytes": 1024,
                "avg_cpu_usage": 80.0,
                "peak_mem_rss_kb": 8192
            }

            perf_file = os.path.join(method_dir, "perf.json")
            with open(perf_file, 'w') as f:
                json.dump(perf_data, f)

            scores_dir = os.path.join(method_dir, "metrics", "partition_metrics", "metric-ari")
            os.makedirs(scores_dir)
            score_file = os.path.join(scores_dir, "clustbench.scores.gz")

            result = extract_performance_data(score_file)
            self.assertEqual(result['runtime'], 30.5)
            self.assertEqual(result['threads'], 4)
            self.assertEqual(result['disk_read'], 2048)
            self.assertEqual(result['disk_write'], 1024)
            self.assertEqual(result['avg_load'], 80.0)
            self.assertEqual(result['peak_rss'], 8192)


class TestDatasetTrueKAndNoise(unittest.TestCase):
    """Test true k and noise extraction from labels"""

    def test_extract_true_k_and_noise_from_labels(self):
        """Test successful extraction of true k and noise from labels file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            dataset_dir = os.path.join(tmpdir, "dataset_generator-fcps_dataset_name-atom")
            os.makedirs(dataset_dir)

            # Create labels file with no noise
            labels_file = os.path.join(dataset_dir, "clustbench.labels.gz")
            with gzip.open(labels_file, 'wt') as f:
                labels = [1, 1, 2, 2, 1, 2, 1, 2]
                for label in labels:
                    f.write(f"{label}\n")

            method_dir = os.path.join(dataset_dir, "clustering", "method-test")
            os.makedirs(method_dir)
            scores_dir = os.path.join(method_dir, "metrics", "partition_metrics", "metric-ari")
            os.makedirs(scores_dir)
            score_file = os.path.join(scores_dir, "clustbench.scores.gz")

            true_k, has_noise = extract_dataset_true_k_and_noise(score_file)
            self.assertEqual(true_k, 2)
            self.assertFalse(has_noise)

    def test_extract_true_k_with_noise_from_labels(self):
        """Test extraction when there is noise in labels file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            dataset_dir = os.path.join(tmpdir, "dataset_generator-sklearn_make_blobs_dataset_name-test")
            os.makedirs(dataset_dir)

            # Create labels file with noise (0 values)
            labels_file = os.path.join(dataset_dir, "clustbench.labels.gz")
            with gzip.open(labels_file, 'wt') as f:
                labels = [1, 1, 2, 0, 1, 2, 0, 2]  # 0 indicates noise
                for label in labels:
                    f.write(f"{label}\n")

            method_dir = os.path.join(dataset_dir, "clustering", "method-test")
            os.makedirs(method_dir)
            scores_dir = os.path.join(method_dir, "metrics", "partition_metrics", "metric-ari")
            os.makedirs(scores_dir)
            score_file = os.path.join(scores_dir, "clustbench.scores.gz")

            true_k, has_noise = extract_dataset_true_k_and_noise(score_file)
            self.assertEqual(true_k, 2)
            self.assertTrue(has_noise)


class TestProcessScoresFile(unittest.TestCase):
    """Integration tests for the main score file processing function"""

    def setUp(self):
        """Set up test directory structure"""
        self.tmpdir = tempfile.mkdtemp()

        # Create dataset directory
        self.dataset_dir = os.path.join(
            self.tmpdir,
            "dataset_generator-fcps_dataset_name-atom"
        )
        os.makedirs(self.dataset_dir)

        # Create labels file for true_k and noise detection
        labels_file = os.path.join(self.dataset_dir, "clustbench.labels.gz")
        with gzip.open(labels_file, 'wt') as f:
            # Write labels for 2 clusters (FCPS atom) with no noise
            labels = [1, 1, 2, 2, 1, 2, 1, 2]
            for label in labels:
                f.write(f"{label}\n")

        # Create method directory
        self.method_dir = os.path.join(
            self.dataset_dir,
            "clustering",
            "method-kmeans_seed-123"
        )
        os.makedirs(self.method_dir)

        # Create clustbench_performance.txt file
        with open(os.path.join(self.method_dir, "clustbench_performance.txt"), 'w') as f:
            f.write("s\tusr\tsys\tmaxrss\tixrss\tidrss\tisrss\tminflt\tmajflt\tnswap\tinblock\toublock\tmsgsnd\tmsgrcv\tnsignals\tnvcsw\tnivcsw\n")
            f.write("30.5\t25.2\t5.3\t4096\t0\t0\t0\t100\t0\t0\t50\t25\t0\t0\t0\t10\t5\n")

        # Create perf.json
        perf_data = {
            "total_time_secs": 30.5,
            "max_threads": 2,
            "total_disk_read_bytes": 2048,
            "total_disk_write_bytes": 1024,
            "avg_cpu_usage": 80.0,
            "peak_mem_rss_kb": 4096
        }
        with open(os.path.join(self.method_dir, "perf.json"), 'w') as f:
            json.dump(perf_data, f)

        # Create metric directory
        self.metric_dir = os.path.join(
            self.method_dir,
            "metrics",
            "partition_metrics",
            "metric-ari"
        )
        os.makedirs(self.metric_dir)

        # Create score file path
        self.score_file = os.path.join(self.metric_dir, "clustbench.scores.gz")

    def tearDown(self):
        """Clean up test directory"""
        import shutil
        shutil.rmtree(self.tmpdir)

    def test_process_scores_file_success(self):
        """Test successful processing of a scores file"""
        # Create sample scores data
        scores_data = [
            ["k=2", "k=3", "k=4", "k=5"],
            ["0.8", "0.9", "0.95", "0.85"]
        ]

        with gzip.open(self.score_file, 'wt') as f:
            writer = csv.writer(f)
            writer.writerows(scores_data)

        method_result, metric_result = process_scores_file(
            self.score_file, "conda", "202506231301", self.tmpdir
        )

        # Test method result
        self.assertIsNotNone(method_result)
        self.assertEqual(method_result['dataset_generator'], "fcps")
        self.assertEqual(method_result['dataset_name'], "atom")
        self.assertEqual(method_result['method'], "kmeans")
        self.assertEqual(method_result['seed'], 123)
        self.assertEqual(method_result['true_k'], 2)
        self.assertFalse(method_result['has_noise'])
        self.assertEqual(method_result['execution_time_seconds'], 30.5)
        self.assertEqual(method_result['runtime'], 30.5)
        self.assertEqual(method_result['threads'], 2)

        # Test metric result
        self.assertIsNotNone(metric_result)
        self.assertEqual(metric_result['metric'], "ari")
        self.assertEqual(metric_result['k=2'], 0.8)
        self.assertEqual(metric_result['k=3'], 0.9)
        self.assertEqual(metric_result['k=4'], 0.95)
        self.assertEqual(metric_result['k=5'], 0.85)
        self.assertFalse(metric_result['duplicate_k_anomaly'])
        self.assertFalse(metric_result['empty_file'])
        self.assertFalse(metric_result['missing_true_k_score'])

    def test_process_scores_file_empty_file(self):
        """Test processing of an empty scores file"""
        # Create empty gzipped file
        with gzip.open(self.score_file, 'wt') as f:
            pass  # Write nothing

        method_result, metric_result = process_scores_file(
            self.score_file, "conda", "202506231301", self.tmpdir
        )

        # Should still return results but mark as empty file
        self.assertIsNotNone(method_result)
        self.assertIsNotNone(metric_result)
        self.assertTrue(metric_result['empty_file'])

    def test_process_scores_file_duplicate_k_anomaly(self):
        """Test detection of duplicate k anomaly"""
        # Create scores data with duplicate k values
        scores_data = [
            ["k=2", "k=3", "k=3", "k=4"],  # Duplicate k=3
            ["0.8", "0.9", "0.85", "0.95"]
        ]

        with gzip.open(self.score_file, 'wt') as f:
            writer = csv.writer(f)
            writer.writerows(scores_data)

        method_result, metric_result = process_scores_file(
            self.score_file, "conda", "202506231301", self.tmpdir
        )

        self.assertIsNotNone(metric_result)
        self.assertTrue(metric_result['duplicate_k_anomaly'])

    def test_process_scores_file_missing_true_k_score(self):
        """Test detection of missing true k score"""
        # Create scores data without k=2 (true_k)
        scores_data = [
            ["k=3", "k=4", "k=5"],
            ["0.9", "0.95", "0.85"]
        ]

        with gzip.open(self.score_file, 'wt') as f:
            writer = csv.writer(f)
            writer.writerows(scores_data)

        method_result, metric_result = process_scores_file(
            self.score_file, "conda", "202506231301", self.tmpdir
        )

        self.assertIsNotNone(metric_result)
        self.assertTrue(metric_result['missing_true_k_score'])


class TestComplexScenarios(unittest.TestCase):
    """Test complex and edge case scenarios"""

    def test_linkage_clustering_method(self):
        """Test processing of linkage-based clustering methods"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create directory structure for linkage method
            linkage_dir = os.path.join(tmpdir, "dataset_generator-fcps_dataset_name-atom",
                                     "clustering", "agglomerative", "linkage-ward")
            os.makedirs(linkage_dir)

            path = os.path.join(linkage_dir, "metrics", "partition_metrics",
                              "metric-ari", "clustbench.scores.gz")
            result = extract_method_info(path)
            self.assertEqual(result['method'], "agglomerative_linkage-ward")

    def test_method_with_parameters_json(self):
        """Test method extraction from parameters.json"""
        with tempfile.TemporaryDirectory() as tmpdir:
            method_dir = os.path.join(tmpdir, "method-test")
            os.makedirs(method_dir)

            # Create parameters.json
            params = {"method": "dbscan", "seed": 456}
            with open(os.path.join(method_dir, "parameters.json"), 'w') as f:
                json.dump(params, f)

            path = os.path.join(method_dir, "metrics", "partition_metrics",
                              "metric-ari", "clustbench.scores.gz")
            result = extract_method_info(path)
            self.assertEqual(result['method'], "dbscan")
            self.assertEqual(result['seed'], 456)


if __name__ == '__main__':
    unittest.main()
