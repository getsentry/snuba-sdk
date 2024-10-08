#!/usr/bin/env python

"""
Snuba-SDK - SnQL SDK for Snuba
=====================================

**Snuba-SDK is an SDK for Snuba.** Check out `GitHub
<https://github.com/getsentry/snuba-sdk>`_ to find out more.
"""

import os

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))


def get_file_text(file_name: str) -> str:
    with open(os.path.join(here, file_name)) as in_file:
        return in_file.read()


setup(
    name="snuba-sdk",
    version="3.0.43",
    author="Sentry",
    author_email="oss@sentry.io",
    url="https://github.com/getsentry/snuba-sdk",
    project_urls={
        "Changelog": "https://github.com/getsentry/snuba-sdk/blob/main/CHANGES.rst",
    },
    description="Snuba SDK for generating SnQL queries.",
    long_description=get_file_text("README.rst"),
    long_description_content_type="text/x-rst",
    packages=find_packages(exclude=("tests", "tests.*")),
    # PEP 561
    package_data={"snuba_sdk": ["py.typed"]},
    zip_safe=False,
    license="FSL-1.0-Apache-2.0",
    install_requires=[
        "parsimonious>=0.10.0",
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
