# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='BTC_testing',
    version='0.0.1',
    description='foobar',
    long_description=readme,
    author='oket',
    author_email='placeholder@placeholder.com',
    url='https://github.com/bigdata-project-march-2018-team1/Visu_Bitcoin',
    license=license,
    packages=find_packages(exclude=('tmp','test','ansible'))
        
)