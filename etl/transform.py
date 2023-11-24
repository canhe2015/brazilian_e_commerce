import os
from dataclasses import dataclass

from pyspark.sql import DataFrame
from utl.utls import rtn_logger, read_yaml, spark, show_df


@dataclass
class YmlConf:
    dataset_root_dir: str
    source_file: dict
    countries: list
    output_path: str
    output_partitions: str
    output_columns: str

    def get_dataset(self, name) -> dict:
        # concat the root directory of csv file with file name and update the dictionary
        data = self.source_file.get(name, None)
        if data:
            data["dataset"] = os.path.join(self.dataset_root_dir, data.get("dataset", None))
        return data


class Transform:
    """
    this is class will perform the ETL process, read the source file and apply filters and transform the data
    """
    def __init__(self, yml_path, env):
        self.yml = None
        self.yml_path = yml_path
        self.logger = rtn_logger("Brazilian_E_Commerce_" + self.__class__.__name__)
        self.env = env

    def construct_yml(self) -> None:
        # read setting.yaml and construct YmlConf instance
        data = read_yaml(self.yml_path).get(self.env, None)
        self.logger.info(f"yaml data is {data}")
        if data:
            self.yml = YmlConf(**data)
        else:
            raise Exception("yaml data is None type")

    def extract(self) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        # extract the data into dataframe according path,columns,filter provided in yaml file
        keys = ["products", "orders", "order_payment", "order_items"]
        datasets = [self.yml.get_dataset(k) for k in keys]
        self.logger.info(f"datasets list is {datasets}")
        if not all(datasets):
            raise Exception("empty datasets are spotted, please provide all required datasets")
        products, orders, order_payment, order_items = [
                spark
                .read
                .format(d['format'])
                .option("header", True)
                .load(d['dataset'])
                .select(*d['columns'])
                .where(d.get('filters', '1=1'))
                for d in datasets
        ]
        return products, orders, order_payment, order_items

    def transform(self, products: DataFrame,
                  orders: DataFrame, order_payment: DataFrame, order_items: DataFrame) -> DataFrame:
        # join the products, order_payment,order_items and orders
        df1 = products.join(order_items, ["product_id"]).select("product_id", "order_id", "product_category_name")
        df1.show(truncate=False)

        df2 = orders.join(order_payment, ['order_id'], "left").select(orders['order_id'],
                                                                      orders['order_purchase_timestamp'],
                                                                      order_payment['payment_value']
                                                                      )
        result = (df1
                  .join(df2, ['order_id'])
                  .selectExpr("order_id",
                              "product_id",
                              "product_category_name",
                              "cast(payment_value as double) as payment_value",
                              "cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp",
                              )
                  )
        self.logger.info(f"schema is: \n {result.schema}")
        self.logger.info(f"the final result is \n {show_df(result)}")
        return result

    def load(self, df: DataFrame) -> None:
        # write the file result to a sink specified in the yaml path, if you want to change the partitions, just
        # change the output_partitions in the yaml file
        df.select(self.yml.output_columns).write.mode("overwrite").partitionBy(self.yml.output_partitions).parquet(
            self.yml.output_path)

    def pipeline(self) -> None:
        self.construct_yml()
        dfs = self.extract()
        result = self.transform(*dfs)
        self.load(result)
