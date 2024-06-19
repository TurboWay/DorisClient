# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time : 2022/6/15 16:53
# @Author : way
# @Site : 
# @Describe:

"""
python setup.py sdist bdist_wheel
twine upload dist/*
twine upload --repository testpypi dist/*
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name = "DorisClient",
    version = "1.2.11",
    description = "python for apache-doris",
    license = "Apache License 2.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url = "https://github.com/TurboWay/DorisClient",
    author = "Way",
    author_email = "1143496751@qq.com",
    packages = find_packages(),
    include_package_data = True,
    platforms = "any",
    install_requires = ["requests", "PyMySQL"]
)

