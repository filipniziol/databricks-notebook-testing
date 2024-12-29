from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.testing import assertDataFrameEqual

def test_merge_table_silver_target_table(spark, dbutils):
    create_source_params = {
        "catalog_name": "tests",
        "schema_name": "test_bronze",
        "table_name": "test_source_table"
    }
    create_source_notebook = "/Workspace/Users/filip.niziol@gmail.com/.bundle/main_testing/dev/files/src/notebooks/catalog_dev/schema_bronze/create_table_bronze_source_table"
    dbutils.notebook.run(create_source_notebook, 60, create_source_params)

    create_target_params = {
        "catalog_name": "tests",
        "schema_name": "test_silver",
        "table_name": "test_target_table"
    }
    create_target_notebook = "/Workspace/Users/filip.niziol@gmail.com/.bundle/main_testing/dev/files/src/notebooks/catalog_dev/schema_silver/create_table_silver_target_table"
    dbutils.notebook.run(create_target_notebook, 60, create_target_params)

    spark.sql(f"""
        INSERT INTO {create_source_params['catalog_name']}.{create_source_params['schema_name']}.{create_source_params['table_name']}
        VALUES
            (1, 'Alpha', current_timestamp(), current_timestamp(), 'active'),
            (2, 'Beta', current_timestamp(), current_timestamp(), 'inactive')
    """)

    spark.sql(f"""
        INSERT INTO {create_target_params['catalog_name']}.{create_target_params['schema_name']}.{create_target_params['table_name']}
        VALUES
            (1, 'Old Alpha', current_timestamp(), current_timestamp(), 'old_status')
    """)

    merge_notebook_params = {
        "catalog_name": create_source_params['catalog_name'],
        "bronze_schema_name": create_source_params['schema_name'],
        "silver_schema_name": create_target_params['schema_name'],
        "source_table_name": create_source_params['table_name'],
        "target_table_name": create_target_params['table_name']
    }

    merge_notebook_path = "/Workspace/Users/filip.niziol@gmail.com/.bundle/main_testing/dev/files/src/notebooks/catalog_dev/schema_silver/merge_table_silver_target_table"
    merge_result = dbutils.notebook.run(merge_notebook_path, 60, merge_notebook_params)

    df_actual = spark.sql(f"""
        SELECT id, name, status
        FROM {create_target_params['catalog_name']}.{create_target_params['schema_name']}.{create_target_params['table_name']}
        ORDER BY id
    """)

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

    assertDataFrameEqual(df_actual, df_expected)
