"""Shopify Shops Scrape."""  # noqa: WPS226
# -*- coding: utf-8 -*-

import logging
from datetime import datetime, timedelta, timezone, date
from types import MappingProxyType
from typing import Generator, Optional, Callable

import httpx
import singer
import time
import collections
import pandas as pd
import requests
from dateutil.parser import isoparse
from dateutil.rrule import DAILY, rrule
from tap_shopify_shops.cleaners import CLEANERS
from google.cloud import bigquery

URL_SCHEME: str = 'https://'
URL_END: str = '/meta.json'

HEADERS: MappingProxyType = MappingProxyType({  # Frozen dictionary
    'Content-Type': 'application/graphql',
    'X-Shopify-Access-Token': ':token:',
})

class Shopify_Shops(object):  # noqa: WPS230
    """Shopify Shops Scrape."""

    def __init__(
        self,
        # organization_id: str,
        # shopify_partners_access_token: str,
    ) -> None:
        """Initialize client.
        Arguments:
            # organization_id {str} -- Shopify Partners organization id
            # shopify_partners_access_token {str} -- Shopify Partners Server Token
        """
        # self.organization_id: str = organization_id
        # self.shopify_partners_access_token: str = shopify_partners_access_token
        self.logger: logging.Logger = singer.get_logger()
        self.client: httpx.Client = httpx.Client(http2=True)

    def shopify_shops(
        self,
        **kwargs: dict,
    )-> Generator[dict, None, None]:
        """
        Shopify Shop meta.json scrape
        """
        self.logger.info('Stream Shopify shop data')

        # Validate the start_date value exists
        start_date_input: str = str(kwargs.get('start_date', ''))

        if not start_date_input:
            raise ValueError('The parameter start_date is required.')

        # Set start date and end date
        start_date: datetime = isoparse(start_date_input)

         # The start date until now function wants a string
        start_date_string = str(start_date)

        # Extra kwargs will be converted to parameters in the API requests
        # start_date is parsed into batches, thus we remove it from the kwargs
        kwargs.pop('start_date', None)


        for date_day in self._start_days_till_now(start_date_string):

            #TODO: fix json location of bigquery credentials once on server
            bqclient = bigquery.Client.from_service_account_json('bigquery_credentials.json')
            #TODO: remove limit when in production
            query_string = """SELECT DISTINCT shop_domain 
                            FROM `yoast-269513.shopify_partners_raw.shopify_partners_app_subscription_charge` LIMIT 10"""
            df_urls = bqclient.query(query_string).result().to_dataframe()

            # create an empty dataframe
            df_results = pd.DataFrame(columns=['id', 'name', 'city', 'province', 
                                        'country', 'currency', 'domain', 'url', 
                                        'myshopify_domain', 'description', 'ships_to_countries', 
                                        'money_format', 'published_collections_count', 
                                        'published_products_count', 
                                        'shopify_pay_enabled_card_brands'])
            
            # loop through each url, make the request, convert to json, append to result df
            
            for url in df_urls.itertuples():
                try:
                    temp_url = URL_SCHEME + url.shop_domain + URL_END
                    self.logger.info(f'~~~~~~URL: {temp_url}')
                    json_response = requests.get(temp_url).json()
                    self.logger.info(f'+++++++Response: {json_response}')
                    df_results = df_results.append([json_response], ignore_index=True)
                    time.sleep(1)
                except:
                    self.logger.info(f"Exception occurred. Failed to scrape: {temp_url}")
            
            # drop the columns we don't care about
            df_results.drop(['ships_to_countries', 'money_format'], axis=1, inplace=True)

            # convert to numeric
            df_results[['id', 'published_collections_count', 'published_products_count']] = df_results[['id', 'published_collections_count', 'published_products_count']].apply(pd.to_numeric)

            # add a column to turn the id into the same url + id format the other tables have
            df_results['shop_id'] = df_results.apply(lambda row: "gid://partners/Shop/" + str(row.id), axis=1)

            # add a date column
            df_results['extracted_at'] = time.strftime("%Y-%m-%d %l:%M:%S %Z")

            # If we ever want to only append new shops to the list, the following is code to do that.
            # Note: would need to add logic to delete the shop from the table if it already exists 
            # and has been grabbed fresh so the new data can replace the old data.
            # # get the shops we've already scraped
            # shop_query_string = """SELECT DISTINCT shop_domain 
            #                     FROM `yoast-269513.shopify_partners_raw.shopify_shop_scrape`"""
            # df_already_scraped_urls = bqclient.query(shop_query_string).result().to_dataframe()

            # # compare already-scraped shops to results, keeping only new entries
            # df_new_entries = df_results.merge(df_already_scraped_urls, left_on='myshopify_domain', 
            #                                     right_on='shop_domain', how='outer', 
            #                                     indicator=True).loc[lambda x:x['_merge']=='left_only']
            # df_new_entries.drop(['_merge'], axis=1, inplace=True)

            # Define cleaner:
            cleaner: Callable = CLEANERS.get('shopify_shops')

            for row in df_results.itertuples():
                yield cleaner(date_day, row)

        self.logger.info('Finished: shopify_shop_scrape')

    # def _create_headers(self) -> None:
    #     """Create authenticationn headers for requests."""
    #     headers: dict = dict(HEADERS)
    #     headers['X-Shopify-Access-Token'] = headers['X-Shopify-Access-Token'].replace(
    #         ':token:',
    #         self.shopify_partners_access_token,
    #     )
    #     self.headers = headers

    def _start_days_till_now(self, start_date: str) -> Generator:
        """Yield YYYY/MM/DD for every day until now.
        Arguments:
            start_date {str} -- Start date e.g. 2020-01-01
        Yields:
            Generator -- Every day until now.
        """
        # Parse input date
        year: int = int(start_date.split('-')[0])
        month: int = int(start_date.split('-')[1].lstrip())
        # day: int = int(start_date.split('-')[2].lstrip())
        # strip the ISO-8601 characters off of the day, leaving only the digit
        # day: int = int(start_date.split('-')[2].rstrip("0:+ ").lstrip("0"))
        day: int = int(start_date.split('-')[2].lstrip("0")[:2].rstrip())

        # Setup start period
        period: date = date(year, month, day)

        # Setup itterator
        dates: rrule = rrule(
            freq=DAILY,
            dtstart=period,
            until=datetime.utcnow(),
        )

        # Yield dates in YYYY-MM-DD format
        yield from (date_day.strftime('%Y-%m-%d') for date_day in dates)