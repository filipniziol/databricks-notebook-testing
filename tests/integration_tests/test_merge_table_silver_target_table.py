# tests/integration_tests/test_merge_table_silver_target_table.py

import os
import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.testing import assertDataFrameEqual

def test_merge_table_silver_target_table(spark, dbutils):
    """
    Test to verify the merge operation between bronze and silver tables.
    This test performs the following steps:
    1. Creates source and target tables by running respective notebooks.
    2. Inserts test data into both tables.
    3. Runs the merge notebook.
    4. Verifies that the target table has the expected data.
    5. Cleans up by dropping the created tables and schema.
    """

    # **1. Parameterize Test Variables**
    catalog_name = "tests"
    bronze_schema_name = "test_bronze"
    silver_schema_name = "test_silver"
    source_table_name = "test_source_table"
    target_table_name = "test_target_table"

    # **2. Dynamically Calculate Root Path**
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    except Exception as e:
        pytest.fail(f"Unable to retrieve notebook path from dbutils: {e}")

    # Calculate the root path relative to the current notebook
    root_path = '/Workspace' + os.path.abspath(os.path.join(notebook_path, "../.."))
    print(f"Root path: {root_path}")

    # **3. Parameterize Notebook Paths**
    create_source_notebook = os.path.join(root_path, "src/notebooks/catalog_dev/schema_bronze/create_table_bronze_source_table")
    create_target_notebook = os.path.join(root_path, "src/notebooks/catalog_dev/schema_silver/create_table_silver_target_table")
    merge_notebook_path = os.path.join(root_path, "src/notebooks/catalog_dev/schema_silver/merge_table_silver_target_table")

    # **4. Verify Notebook Existence**
    assert os.path.exists(create_source_notebook), f"Notebook does not exist: {create_source_notebook}"
    assert os.path.exists(create_target_notebook), f"Notebook does not exist: {create_target_notebook}"
    assert os.path.exists(merge_notebook_path), f"Notebook does not exist: {merge_notebook_path}"

    # **5. Define Parameters for Creating Tables**
    create_source_params = {
        "catalog_name": catalog_name,
        "schema_name": bronze_schema_name,
        "table_name": source_table_name
    }
    create_target_params = {
        "catalog_name": catalog_name,
        "schema_name": silver_schema_name,
        "table_name": target_table_name
    }

    try:
        # **6. Run Notebooks to Create Tables**
        print(f"Running create_source_notebook: {create_source_notebook}")
        dbutils.notebook.run(create_source_notebook, timeout_seconds=60, arguments=create_source_params)

        print(f"Running create_target_notebook: {create_target_notebook}")
        dbutils.notebook.run(create_target_notebook, timeout_seconds=60, arguments=create_target_params)

        # **7. Insert Test Data into Source Table**
        print("Inserting test data into source table.")
        spark.sql(f"""
            INSERT INTO {catalog_name}.{bronze_schema_name}.{source_table_name}
            VALUES
                (1, 'Alpha', current_timestamp(), current_timestamp(), 'active'),
                (2, 'Beta', current_timestamp(), current_timestamp(), 'inactive')
        """)

        # **8. Insert Test Data into Target Table**
        print("Inserting test data into target table.")
        spark.sql(f"""
            INSERT INTO {catalog_name}.{silver_schema_name}.{target_table_name}
            VALUES
                (1, 'Old Alpha', current_timestamp(), current_timestamp(), 'old_status')
        """)

        # **9. Define Parameters for Merge Notebook**
        merge_notebook_params = {
            "catalog_name": catalog_name,
            "bronze_schema_name": bronze_schema_name,
            "silver_schema_name": silver_schema_name,
            "source_table_name": source_table_name,
            "target_table_name": target_table_name
        }

        # **10. Run Merge Notebook**
        print(f"Running merge_notebook_path: {merge_notebook_path}")
        merge_result = dbutils.notebook.run(merge_notebook_path, timeout_seconds=60, arguments=merge_notebook_params)
        print(f"Merge notebook result: {merge_result}")

        # **11. Fetch Actual Data from Target Table**
        print("Fetching actual data from target table.")
        df_actual = spark.sql(f"""
            SELECT id, name, status
            FROM {catalog_name}.{silver_schema_name}.{target_table_name}
            ORDER BY id
        """)

        # **12. Define Expected Data**
        expected_data = [
            Row(id=1, name='Alpha', status='active'),
            Row(id=2, name='Beta', status='inactive')
        ]
        expected_schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('name', StringType(), True),
            StructField('status', StringType(), True)
        ])
        df_expected = spark.createDataFrame(expected_data, expected_schema)

        # **13. Compare DataFrame Schemas**
        print("Comparing DataFrame schemas.")
        assert df_actual.schema == df_expected.schema, "Schemas do not match."

        # **14. Compare Actual Data with Expected Data**
        print("Asserting that actual data matches expected data.")
        assertDataFrameEqual(df_actual, df_expected)

        print("Merge notebook ran successfully and data is as expected!")

    finally:
        # **15. Cleanup: Drop Source and Target Tables and Schema**
        print("Cleaning up test environment.")
        try:
            # Drop source table
            spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{bronze_schema_name}.{source_table_name}")
            # Drop target table
            spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{silver_schema_name}.{target_table_name}")
            print("Cleanup successful.")
        except Exception as e:
            print(f"Error during cleanup: {e}")