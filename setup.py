#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="KubeJobSub",
    version="0.1.13",
    packages=find_packages(),
    scripts=['KubeJobSub/KubeJobSub', 'KubeJobSub/AzureStorage', 'KubeJobSub/AzureBatch'],
    author="Andrew Low",
    author_email="andrew.low@canada.ca",
    url="https://github.com/lowandrew/KubeJobSub",
    install_requires=['pyyaml',
                      'azure-storage-file',
                      'termcolor',
                      'azure-batch',
                      'azure-common',
                      'azure-storage',
                      'pytest']
)
