import os

import pytest
import yaml

from utl.utls import read_yaml, rtn_logger, spark, show_df


def test_read_valid_yaml_file():
    # Create a temporary YAML file for testing
    temp_yaml_file = "temp.yaml"
    yaml_content = {"key": "value"}

    with open(temp_yaml_file, "w") as f:
        yaml.dump(yaml_content, f)

    # Test the read_yaml function with the temporary YAML file
    result = read_yaml(temp_yaml_file)
    os.remove(temp_yaml_file)  # Clean up the temporary file

    assert result == yaml_content


def test_read_invalid_yaml_file():
    # Create an invalid YAML file for testing
    temp_yaml_file = "temp_invalid.yaml"
    invalid_yaml_content = "{'key: value"  # Invalid YAML content

    with open(temp_yaml_file, "w") as f:
        f.write(invalid_yaml_content)

    # Test whether the read_yaml function raises an exception when reading the invalid file
    with pytest.raises(yaml.YAMLError):
        read_yaml(temp_yaml_file)

    os.remove(temp_yaml_file)  # Clean up the temporary file


def test_rtn_logger():
    logger_name = "TestLogger"
    logger = rtn_logger(logger_name)

    assert logger, "Logger object should not be None"
    log_manager = spark.sparkContext._jvm.org.apache.log4j.LogManager
    assert log_manager.exists(logger_name), "Logger name should match the given name"


def test_show_df():
    # Create a sample DataFrame for testing
    data = [("Alice", 1), ("Bob", 2), ("Carol", 3)]
    columns = ["Name", "Value"]
    df = spark.createDataFrame(data, columns)

    # Test show_df function with default parameters
    result_default = show_df(df)
    assert (
            "Alice" in result_default and "Bob" in result_default and "Carol" in result_default
    ), "The result should contain the data from the DataFrame"

    # Test show_df function with n=2
    result_n = show_df(df, n=2)
    assert (
            "Alice" in result_n and "Bob" in result_n and "Carol" not in result_n
    ), "The result should contain data up to n=2"

    # Test show_df function with truncate=True
    result_truncate = show_df(df, truncate=True)
    assert (
            "Alice" in result_truncate and "Bob" in result_truncate and "Carol" in result_truncate
    ), "The result should contain the data from the DataFrame and be truncated"

    # Test show_df function with vertical=True
    result_vertical = show_df(df, vertical=True)
    assert (
            "Alice" in result_vertical and "Bob" in result_vertical and "Carol" in result_vertical
    ), "The result should contain the data from the DataFrame and be displayed vertically"
