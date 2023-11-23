import yaml

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
_sc = spark.sparkContext
_sc.setLogLevel("INFO")


def rtn_logger(name):
    """
       generated logger
       :param name: give a name for logger
       :return: log4j logger with a name specified
    """
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(name)
    return logger


def show_df(df, n=20, truncate=False, vertical=False) -> str:
    """
    display dataframe
    :param df: expect a spark dataframe
    :param n:
    :param truncate:
    :param vertical:
    :return:
    """
    # only to show maximum 50 rows
    if n > 50:
        n = 20
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, truncate, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)


def read_yaml(yaml_file_path: str) -> dict:
    """
        read yaml function from configuration file
        :param yaml_file_path:
        :return:
    """
    with open(yaml_file_path, "r") as f:
        data = yaml.safe_load(f.read())
        print(data)
        return data

