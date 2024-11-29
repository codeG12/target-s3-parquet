#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-s3-parquet",
    version="0.1.0",
    description="Singer.io target for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target-s3-parquet"],
    install_requires=[
        "singer-python>=5.0.12",
    ],
    entry_points="""
    [console_scripts]
    target-s3-parquet=target-s3-parquet:main
    """,
    packages=["target-s3-parquet"],
    package_data = {},
    include_package_data=True,
)
