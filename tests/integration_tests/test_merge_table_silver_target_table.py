def test_merge_table_silver_target_table(spark, dbutils):
    notebook_params = {
        "catalog_name": "dev",
        "bronze_schema_name": "bronze",
        "silver_schema_name": "silver",
        "source_table_name": "source_table",
        "target_table_name": "target_table"
    }

    # Run the notebook with the specified parameters
    result = dbutils.notebook.run("merge_table_silver_target_table", 60, notebook_params)

    assert 0 == 0, "Merge notebook did not produce any rows!"
