#!/usr/bin/env python

from setuptools import setup

setup(name='tap-taboola',
      version='0.3.1',
      description='Singer.io tap for extracting data from the Taboola API',
      author='Fishtown Analytics',
      url='http://www.singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_taboola'],
      install_requires=[
          'singer-python==6.1.1',
          'backoff==2.2.1',
          'requests==2.32.3',
          'python-dateutil==2.9.0'
      ],
      extras_require={
        "dev": [
            "pylint",
            "ipdb",
        ]
        },
      entry_points='''
          [console_scripts]
          tap-taboola=tap_taboola:main
      ''',
      package_data={
        "tap_taboola/schemas": [
            "campaigns.json",
            "campaign_performance.json"
        ],
        },
      include_package_data=True,
      packages=['tap_taboola']
)
