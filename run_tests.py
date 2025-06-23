#!/usr/bin/env python3
"""
Test runner for the clustbench aggregation refactoring.

This script runs all unit tests and provides setup instructions.
"""

import sys
import os
import subprocess
import unittest

def setup_test_environment():
    """Setup the test environment and check dependencies"""
    print("Setting up test environment...")

    # Check if required packages are available
    required_packages = ['pandas', 'numpy']
    missing_packages = []

    for package in required_packages:
        try:
            __import__(package)
            print(f"✓ {package} is installed")
        except ImportError:
            missing_packages.append(package)
            print(f"✗ {package} is missing")

    if missing_packages:
        print(f"\nMissing packages: {', '.join(missing_packages)}")
        print("Please install them with:")
        print(f"pip install {' '.join(missing_packages)}")
        return False

    print("All dependencies are available.")
    return True

def run_tests():
    """Run all unit tests"""
    print("\n" + "="*60)
    print("RUNNING CLUSTBENCH AGGREGATION TESTS")
    print("="*60)

    # Discover and run tests
    test_dir = os.path.join(os.path.dirname(__file__), 'tests')
    loader = unittest.TestLoader()
    suite = loader.discover(test_dir, pattern='test_*.py')

    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)

    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")

    if result.failures:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            print(f"- {test}")

    if result.errors:
        print("\nERRORS:")
        for test, traceback in result.errors:
            print(f"- {test}")

    success = len(result.failures) == 0 and len(result.errors) == 0
    print(f"\nOVERALL: {'PASSED' if success else 'FAILED'}")

    return success

def main():
    """Main function"""
    print("Clustbench Aggregation Test Runner")
    print("==================================")

    # Setup environment
    if not setup_test_environment():
        sys.exit(1)

    # Run tests
    success = run_tests()

    if success:
        print("\n🎉 All tests passed!")
    else:
        print("\n❌ Some tests failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()
