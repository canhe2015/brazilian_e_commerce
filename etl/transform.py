import os
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame
from utl.utls import rtn_logger, read_yaml, spark


@dataclass
class YmlConf:
    dataset_root_dir: str
    source_file: dict
    countries: list

    def get_dataset(self, name) -> dict:
        data = self.source_file.get(name, None)
        print(data)
        if data:
            data["dataset"] = os.path.join(self.dataset_root_dir, data.get("dataset", None))
        return data


class Transform:
    def __init__(self, yml_path):
        self.yml = None
        self.yml_path = yml_path
        self.logger = rtn_logger(self.__class__.__name__)

    def construct_Yml(self) -> None:
        data = read_yaml(self.yml_path)
        self.yml = YmlConf(**data)

    def extract(self) -> tuple[Any, Any, Any, Any]:
        keys = ["products", "orders", "order_payment", "order_items"]
        datasets = [self.yml.get_dataset(k) for k in keys]
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

        products.show(truncate=False)
        orders.show(truncate=False)
        order_payment.show(truncate=False)
        order_items.show(truncate=False)
        return products, orders, order_payment, order_items

    def transform(self, ):
        pass

    def load(self, df: DataFrame) -> None:
        pass

    def pipeline(self) -> None:
        self.construct_Yml()
        self.extract()


if __name__ == "__main__":
    config_dir = os.path.join(os.path.dirname(__file__), "..", "config")
    settings_file = os.path.join(config_dir, "settings.yaml")

    t = Transform(settings_file)
    t.pipeline()
    spark.stop()
