import os
from dataclasses import dataclass

from pyspark.sql import DataFrame
from utl.utls import rtn_logger, read_yaml, spark


@dataclass
class YmlConf:
    dataset_root_dir: str
    source_file: dict
    countries: list
    output_path: str

    def get_dataset(self, name) -> dict:
        data = self.source_file.get(name, None)
        if data:
            data["dataset"] = os.path.join(self.dataset_root_dir, data.get("dataset", None))
        return data


class Transform:
    def __init__(self, yml_path, env):
        self.yml = None
        self.yml_path = yml_path
        self.logger = rtn_logger("Brazilian_E_Commerce_" + self.__class__.__name__)
        self.env = env

    def construct_Yml(self) -> None:
        data = read_yaml(self.yml_path).get(self.env, None)
        self.logger.info(f"yaml data is {data}")
        if data:
            self.yml = YmlConf(**data)
        else:
            raise Exception("yaml data is None type")

    def extract(self) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
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

    def transform(self, products: DataFrame, orders: DataFrame, order_payment: DataFrame, order_items: DataFrame):
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
        result.printSchema()
        result.show(truncate=False)
        return result

    def load(self, df: DataFrame) -> None:
        df.write.mode("overwrite").partitionBy("product_category_name").parquet(self.yml.output_path)

    def pipeline(self) -> None:
        self.construct_Yml()
        products, orders, order_payment, order_items = self.extract()
        result = self.transform(products, orders, order_payment, order_items)
        self.load(result)
