import dataclasses
import json
import os

import pytest

from etl.transform import Transform

d = {'DEV': {'dataset_root_dir': 'C:\\data\\test_data\\Brazilian_E_Commerce',
             'source_file': {'orders':
                                 {'format': 'csv', 'dataset': 'olist_orders_dataset.csv',
                                  'columns': ['order_id', 'order_purchase_timestamp', 'order_status'],
                                  'filters': "order_status not in ('canceled')"},
                             'products':
                                 {'format': 'csv', 'dataset': 'olist_products_dataset.csv',
                                  'columns': ['product_id', 'product_category_name'],
                                  'filters': 'product_category_name is not null'},
                             'order_payment':
                                 {'format': 'csv', 'dataset': 'olist_order_payments_dataset.csv',
                                  'columns': ['order_id', 'payment_value']},
                             'order_items': {'format': 'csv', 'dataset': 'olist_order_items_dataset.csv',
                                             'columns': ['order_id', 'product_id']}},
             'countries': ['brazil'], 'output_path': 'C:\\data\\test_data\\output',
             'output_partitions': 'product_id',
             'output_columns': ['product_id', 'order_id', 'product_category_name', 'order_purchase_timestamp',
                                'payment_value']}
     }


@pytest.fixture
def make_transform_instance(tmpdir):
    tmp_yml_path = os.path.join(tmpdir, "test.yaml")
    with open(tmp_yml_path, "w") as f:
        json.dump(d, f)
    return Transform(tmp_yml_path, "DEV")


def test_construct_Yml(make_transform_instance):
    make_transform_instance.construct_yml()
    assert dataclasses.asdict(make_transform_instance.yml) == d['DEV']
