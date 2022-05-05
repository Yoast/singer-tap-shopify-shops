"""Shopify Partners tap."""
# -*- coding: utf-8 -*-
import logging
from argparse import Namespace

import pkg_resources
from singer import get_logger, utils
from singer.catalog import Catalog

from tap_shopify_shops.scrape import Shopify_Shops
from tap_shopify_shops.discover import discover
from tap_shopify_shops.sync import sync

VERSION: str = pkg_resources.get_distribution('tap-shopify-shops').version
LOGGER: logging.RootLogger = get_logger()
REQUIRED_CONFIG_KEYS: tuple = (
    'start_date',
)


@utils.handle_top_exception(LOGGER)
def main() -> None:
    """Run tap."""
    # Parse command line arguments
    args: Namespace = utils.parse_args(REQUIRED_CONFIG_KEYS)

    LOGGER.info(f'>>> Running tap-shopify-shops v{VERSION}')

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog: Catalog = discover()
        catalog.dump()
        return

    # Otherwise run in sync mode
    if args.catalog:
        # Load command line catalog
        catalog = args.catalog
    else:
        # Load the  catalog
        catalog = discover()
    
    # Initialize Shopify Shops object
    shopify_shops: Shopify_Shops = Shopify_Shops()

    sync(shopify_shops, args.state, catalog, args.config['start_date'])


if __name__ == '__main__':
    main()