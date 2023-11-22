import yaml

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
_sc = spark.sparkContext
_sc.setLogLevel("INFO")


def rtn_logger(name) -> :
    """
       generated logger
       :param name:
       :return:
    """
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(name)
    return logger


def show_df(df, n=20, truncate=False, vertical=False) -> None:
    """
    display dataframe
    :param df:
    :param n:
    :param truncate:
    :param vertical:
    :return:
    """
    if n > 50:
        n = 20
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 23, vertical)
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
        return data


# class SparkClz:
#     def __init__(self, appname, master="local[*]"):
#         self.appname = appname
#         self.master = master
#
#     def rt_spk_ins(self):
#         return (SparkSession
#                 .builder
#                 .master(self.master)
#                 .appName(self.appname)
#                 .enableHiveSupport()
#                 .getOrCreate())
