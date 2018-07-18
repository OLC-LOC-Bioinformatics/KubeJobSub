#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="KubeJobSub",
    version="0.1.1",
    packages=find_packages(),
    scripts=['KubeJobSub/KubeJobSub'],
    author="Andrew Low",
    author_email="andrew.low@canada.ca",
    url="https://github.com/lowandrew/KubeJobSub",
    install_requires=['pyyaml']
)
