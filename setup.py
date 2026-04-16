#!/usr/bin/env python

from setuptools import setup

setup(name="tap-taboola",
      version="1.1.0",
      description="Singer.io tap for extracting data from the Taboola API",
      author="Fishtown Analytics",
      url="http://www.singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_taboola"],
      install_requires=[
          "singer-python==6.8.0",
          "backoff==2.2.1",
          "requests==2.32.5",
          "python-dateutil==2.9.0"
      ],
      extras_require={
        "dev": [
            "pylint",
            "ipdb",
            "pytest",
            "coverage",
        ]
        },
      entry_points="""
          [console_scripts]
          tap-taboola=tap_taboola:main
      """,
      package_data={
        "tap_taboola": ["schemas/*.json"],
        },
      include_package_data=True,
      packages=["tap_taboola"]
)
