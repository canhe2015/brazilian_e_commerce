from setuptools import setup, find_packages
import addtag

setup(
    name='dmp_d_dataprocessor',
    version=addtag.__version__,
    description='dmpd realtime tagging',
    packages=find_packages(include=['addtag', 'utl', 'pipeline','crm']),
    entry_points={
        'group_1': 'run=addtag.__main__:main'

    },
    install_requires=[
        'setuptools'
    ]
)
