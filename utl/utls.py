from typing import Any

import yaml

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
_sc = spark.sparkContext
_sc.setLogLevel("INFO")


def rtn_logger(name) -> Any:
    """
       generated logger
       :param name: give a name for logger
       :return: java log4j logger instance with a name specified
    """
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(name)
    return logger


def show_df(df, n=20, truncate=False, vertical=False) -> str:
    """
    display dataframe
    :param df: expect a spark dataframe
    :param n: by default only show 20 rows
    :param truncate: refer to df.show method
    :param vertical: refer to df.show method
    :return: string
    """
    # only to show maximum 50 rows
    if n > 50:
        n = 20
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 20, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)


def read_yaml(yaml_file_path: str) -> dict:
    """
        read yaml function from configuration file
        :param yaml_file_path:
        :return: a dictionary
    """
    with open(yaml_file_path, "r") as f:
        data = yaml.safe_load(f.read())
        print(data)
        return data

