#!/usr/bin/env python

from setuptools import setup

setup(
    name='thrift-logger',
    version='0.0.1',
    description='ThiftLogger for Singer',
    author='Pinterest, inc.',
    url='http://www.pinterest.com',
    install_requires=[
        'pinstatsd==1.0.55',
        'thrift==0.8.0'
    ],
    packages=['thrift_logger', 'thrift_logger/thrift_libs'])
