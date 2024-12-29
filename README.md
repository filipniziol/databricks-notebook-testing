# Writing Integration Tests for Databricks SQL Notebooks

This guide demonstrates how to effectively write integration tests for Databricks SQL notebooks, using a practical example of testing merge operations between bronze and silver tables.

## Repository Structure

The example repository `main_testing` showcases a complete setup for testing Databricks notebooks:

```
main_testing/
├── tests/
│   ├── integration_tests/
│   │   ├── conftest.py
│   │   └── test_merge_table_silver_target_table.py
│   └── run_integration_tests.py
└── src/
    └── notebooks/
        └── catalog_dev/
            ├── schema_bronze/
            │   └── create_table_bronze_source_table
            └── schema_silver/
                ├── create_table_silver_target_table
                └── merge_table_silver_target_table
```

## Key Components

### 1. Test Runner Setup

The `run_integration_tests.py` configures the test environment and executes all integration tests:

```python
import pytest
import sys

# Integration tests folder relative to this script
integration_tests_folder = "tests/integration_tests"

# Avoid writing .pyc files on a read-only filesystem.
sys.dont_write_bytecode = True

# Run pytest on integration tests folder
retcode = pytest.main([
    integration_tests_folder, 
    "-v", 
    "-p", "no:cacheprovider", 
    "--junit-xml", "/dbfs/tmp/integration_tests/junit.xml"
])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."
```

### 2. Fixtures Configuration

`conftest.py` provides essential test fixtures:

- **SparkSession Initialization**
- **DBUtils Access for Notebook Operations**

```python
# tests/integration_tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    # Initialize SparkSession
    spark = SparkSession.builder.appName('integration-tests').getOrCreate()
    yield spark
    # Teardown: Stop SparkSession after tests
    spark.stop()

@pytest.fixture
def dbutils(spark):
    # Provide DBUtils
    from pyspark.dbutils import DBUtils
    return DBUtils(spark)
```

### 3. Integration Test Example

The main test file `test_merge_table_silver_target_table.py` demonstrates a comprehensive approach to testing a merge operation:

#### Key Features:

- **Table Creation and Cleanup**
- **Test Data Insertion**
- **Notebook Execution with Parameters**
- **Data Validation Using PySpark Testing Utilities**

## Test Flow

1. **Setup:** Creates test tables.
2. **Data Preparation:** Inserts test data into source and target tables.
3. **Execute:** Runs the merge notebook.
4. **Validate:** Compares actual results with expected data.
5. **Cleanup:** Removes test tables.

## Best Practices Demonstrated

- **Parameterization:** All table names and paths are parameterized.
- **Error Handling:** Proper cleanup in `finally` blocks.
- **Assertions:** Clear validation of results.

## Running the Tests

To run the integration tests in your Databricks environment:

1. **Clone the Repository**
2. **Upload the Code to Your Databricks Workspace**
3. **Create Test Catalog and Schemas**
4. **Run the `run_integration_tests.py` Notebook**

## Real-World Application

This testing framework is particularly useful for:

- **ETL Pipeline Validation**
- **Data Quality Checks**
- **Regression Testing**

## Benefits

- **Catches Integration Issues Early**
- **Ensures Data Transformation Accuracy**
- **Provides Documentation Through Tests**
- **Enables Confident Refactoring**
- **Supports CI/CD Pipeline Integration**

For a complete working example, visit the [main_testing repository](https://github.com/your-repo/main_testing).

This testing approach ensures reliable and maintainable data pipelines in Databricks environments while following software engineering best practices.

---

**Additional Note:**

While this guide refers to the tests as "integration tests" due to the involvement of the Spark context, it's important to note that these tests function similarly to unit tests by focusing on individual notebooks. This terminology can be adjusted based on your project's specific needs and conventions.

---

**Recommendations:**

1. **Use Logging Instead of Print Statements:**
   - Replace `print` statements with Python's `logging` module for better control over log levels and formats.
   - Example:
     ```python
     import logging

     # Configure logging
     logging.basicConfig(level=logging.INFO)
     logger = logging.getLogger(__name__)

     # Replace print with logger
     logger.info("Running create_source_notebook: %s", create_source_notebook)
     ```

2. **Externalize Configuration:**
   - Move notebook paths and other configurations to external configuration files (e.g., YAML, JSON) or environment variables for greater flexibility.

3. **Enhanced Exception Handling:**
   - Implement more granular exception handling to provide clearer error messages and better diagnostics.

4. **Consistent Naming Conventions:**
   - Ensure consistency in naming conventions across your project for better maintainability.

---

Feel free to modify this README to better fit your project's structure and requirements. If you have any further questions or need additional assistance, don't hesitate to reach out!