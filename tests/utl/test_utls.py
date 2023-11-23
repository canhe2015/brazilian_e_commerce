import os

import pytest
import yaml

from utl.utls import read_yaml


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
