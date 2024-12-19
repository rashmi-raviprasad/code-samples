'''This script extracts, transforms, and loads data from Shopify API to
obtain daily product inventory levels'''
import gzip
import json
from datetime import datetime as dt
from datetime import timedelta, timezone

from flask import current_app as app

from sample_framework.redshift_loader.main import pipe as redshift_loader
from sample_framework.logging import get_cloud_logger, slack_message
from sample_framework.requests import request_with_retries

NAME = 'Shopify Inventory Data Extract'
DESCRIPTION = 'Daily extraction of Shopify stock data'
ARGUMENTS = ''
GROUP = 'DTC'
PLATFORM = 'Shopify'

CREDENTIALS = app.config['CREDENTIALS'][PLATFORM]

SHOPIFY_API = "https://sample-brand.myshopify.com/admin/api"
VERSION = "2021-07"
INVENTORY_ENDPOINT = "inventory_levels.json"

COLUMN_MAP = {
    "report_date": "",
    "inventory_item_id": "inventory_item_id",
    "quantity_on_hand": "available"
}

REDSHIFT_TABLE = "shopify_sample_table_name"

UTC = timezone.utc


def handle(context, task_id, **kwargs):
    '''
    Handles logic to extract data with the correct dates, upload the data to S3
    '''
    global LOGGER
    LOGGER = get_cloud_logger(__name__)
    LOGGER.info('start extraction of Shopify inventory data')

    # Extract Shopify product inventory
    report_date = (dt.now() - timedelta(1)).date()
    data = extract_data()
    # Upload raw data to S3
    put_s3(json.dumps(data), 'raw-data', report_date)
    # Parse inventory data
    report = parse_inventory(data, report_date)
    # Upload transformed data to S3
    LOGGER.info('uploading data to S3')
    copy_data = gzip.compress(
        bytes(copy_format(report), 'utf-8'))
    s3_key = put_s3(copy_data, 'transformed-data', report_date)
    copy_args = {
        'table_name': REDSHIFT_TABLE,
        's3_key': s3_key
    }
    redshift_loader.apply_async((copy_args, context))

    slack_message(f":white_check_mark: {NAME} complete!")


def extract_data():
    '''
    Builds and sends the request to get Shopify inventory
    '''
    LOGGER.info('requesting Shopify daily stock data')
    # Build URL endpoint
    url = f"{SHOPIFY_API}/{VERSION}/{INVENTORY_ENDPOINT}"
    # Build headers from app credentials
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": CREDENTIALS['TOKEN']
    }
    # Prepare query string parameters
    params = {
        "location_ids": "sample_id",
        "limit": 250
    }

    # Extract data with parameters
    res = request_with_retries(
        'GET',
        url=url,
        headers=headers,
        params=params
    )

    # Extract data with parameters
    # Looping through pages
    finished = False
    inventory_levels = list()
    while not finished:
        res = request_with_retries(
            'GET',
            url=url,
            headers=headers,
            params=params
        )
        # Raise error if response code is not 200
        res.raise_for_status()
        # Return JSON format of response data
        inventory_levels.extend(res.json()['inventory_levels'])
        # Check for more pages
        link_header = res.headers.get('Link')
        if link_header is None or 'rel="next"' not in link_header:
            finished = True
            # If no 'Link' in response header, exit loop
            # If 'Link' header does not have 'rel=next', exit loop
        else:
            rel_links = link_header.split(', ')
            for link in rel_links:
                if 'next' in link:
                    next_link, _ = link.split(';')
                    url = next_link.strip('<>')
                    params = None
    return inventory_levels


def parse_inventory(stock_list: dict, report_date: dt.date) -> dict:
    '''
    Process data to transform product level records
    '''
    LOGGER.info('parsing Shopify daily stock data')
    stock_data = list()
    # Iterate through all inventory items
    for product in stock_list:
        stock_info = {
            field: product.get(COLUMN_MAP[field])
            if COLUMN_MAP[field] else COLUMN_MAP[field]
            for field in COLUMN_MAP}
        stock_info['report_date'] = f'{report_date}'
        stock_info['created_at'] = dt.now().astimezone(
            UTC).strftime('%Y-%m-%d %H:%M:%S')
        stock_data.append(stock_info)
    return stock_data


def copy_format(data):
    '''
    Format data prescribed by Redshift for Copy Command
    '''
    return '\n'.join(json.dumps(item) for item in data)


def put_s3(data: str, location: str, date: str):
    '''
    Upload data to appropriate S3 directory with given arguments
    '''
    now = dt.now()
    compression = ''
    if location == 'transformed-data':
        compression = '.gz'

    key = f'{location}/sample/path/' \
        f'year={date.year}/month={date.month}/day={date.day}/' \
        f'sample_table_{date}_{now}.json{compression}'

    app.s3_client.put_object(
        Bucket=app.config['S3_BUCKET'],
        Key=key,
        Body=data,
        ContentType='application/json'
    )
    return key
