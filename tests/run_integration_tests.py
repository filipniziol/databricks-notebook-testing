# Databricks notebook source
%reload_ext autoreload
%autoreload 2

# COMMAND ----------

import pytest
import os
import sys

# Integration tests folder
integration_tests_folder = "integration_tests"

# Avoid writing .pyc files on a read-only filesystem.
sys.dont_write_bytecode = True

# Run pytest on integration tests folder
retcode = pytest.main([integration_tests_folder, "-v", "-p", "no:cacheprovider", "--junit-xml", "/dbfs/tmp/integration_tests/junit.xml"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."
