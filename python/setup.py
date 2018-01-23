#!/usr/bin/env python

from setuptools import setup

# monkey-patch out the semantic versioning functionality of pbr
from functools import wraps
import pbr.packaging

@wraps(pbr.packaging.get_version)
def new_get_version(package_name, pre_version=None):
    import sys
    if pre_version:
        return pre_version
    raise Exception("please ensure the setup configuration includes an explicit version")


pbr.packaging.get_version = new_get_version


setup(
    setup_requires=['pbr'],
    pbr=True,
)
