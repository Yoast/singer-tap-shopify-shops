"""Setup."""
# -*- coding: utf-8 -*-
from setuptools import find_packages, setup

setup(
    name='tap-shopify-shops',
    version='0.1.0',
    description='Singer.io tap for scraping data about Shopify shops',
    author='Yoast',
    url='https://github.com/Yoast/singer-tap-shopify-shops',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_shopify_shops'],
    install_requires=[
        'httpx[http2]~=0.16.1',
        'python-dateutil~=2.8.1',
        'singer-python~=5.10.0',
        'google-cloud-bigquery~=3.0.0',
        'pandas~=1.3.5'
    ],
    entry_points="""
        [console_scripts]
        tap-shopify-shops=tap_shopify_shops:main
    """,
    packages=['tap_shopify_shops'],
    package_data={
        'schemas': ['tap_shopify_shops/schemas/*.json'],
    },
    include_package_data=True,
)