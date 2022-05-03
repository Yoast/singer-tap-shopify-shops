# singer-tap-shopify-shops
This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#singer-specification).

This tap:

- Scrapes data from the /meta.json file for each Shopify shop in a given list
- Extracts the following resources:
  - Shop location
  - Domain
  - Description
- Outputs the schema for each resource
- Adds only new data to the BigQuery table
### Step 1: Get a list of available shops via a Shopify Partners Query
1. TODO



### Step 2: Scrape
1. TODO
Copyright © 2021 Yoast