from setuptools import setup, find_packages

import etl

setup(
    name='brazilian_e_commerce',
    version=etl.__version__,
    description='brazilian_e_commerce',
    packages=find_packages(include=['etl', 'utl', 'tests']),
    entry_points={
        'group_1': 'run=etl.__main__:main'

    },
    install_requires=[
        'setuptools'
    ]
)
