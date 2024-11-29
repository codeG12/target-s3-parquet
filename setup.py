#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name="target-s3-parquet",
      version="0.0.1",
      python_requires=">=3.7.0, <3.11",
      description="Singer.io target for writing parquet files and upload to S3 - PipelineWise compatible",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="Wise",
      url='https://github.com/codeG12/target-s3-parquet',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10'
      ],
      py_modules=["target_s3_parquet"],
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'inflection==0.5.1',
          'boto3==1.17.39',
          'pandas==1.3.0',
          'pyarrow==11.0.0'
      ],
      extras_require={
          "test": [
              'pylint==2.10.*',
              'pytest==6.2.*',
              'pytest-cov==2.12.*',
          ]
      },
      entry_points="""
          [console_scripts]
          target-s3-parquet=target_s3_parquet:main
       """,
      packages=["target_s3_parquet"],
      package_data={},
      include_package_data=True,
      )

