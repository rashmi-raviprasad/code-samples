'''
Classifies new Shopify products daily. Tags are loaded to the
shopify_product_classification table in Redshift.
'''

import datetime as dt
import gzip
import json

from flask import current_app as app

from sample_framework.db import db
from sample_framework.redshift import (ShopifyProductListing,
                                        ShopifyProductTags)
from sample_framework.redshift_loader import pipe as redshift_loader
from sample_framework.logging import get_cloud_logger, slack_message
from sample_framework.product_classifier import classify

NAME = 'Shopify Product Classifier'
DESCRIPTION = 'Daily classification of Shopify products'
ARGUMENTS = 'count'
GROUP = 'DTC'

DESTINATION_TABLE = 'shopify_sample_table'


def handle(context, task_id, count=None, **kwargs) -> None:
    '''
    Handler for Shopify product classification job.
    '''
    global LOGGER
    LOGGER = get_cloud_logger(__name__)

    LOGGER.info('getting new products for classification...')
    new_products = get_new_products(count)
    LOGGER.info('found %s new products...', len(new_products))

    if len(new_products) == 0:
        # Stop the job if no new products are found
        slack_message(
            f'{app.config["WARNING_ICON"]} No new Shopify products found'
        )
        return

    LOGGER.info('tagging new products...')
    tagged_products = classify(new_products, 2, 5)
    LOGGER.info('tagged %s new products...', len(tagged_products))

    transformed_products = transform_product_tags(
        tagged_products, new_products)

    # Convert tagged product data to copy command format
    copy_format = '\n'.join([json.dumps(item)
                             for item in transformed_products])
    copy_data = gzip.compress(bytes(copy_format, 'utf-8'))

    LOGGER.info('uploading tagged product data to S3...')
    s3_key = upload_data(copy_data)
    LOGGER.info('submitting load job')
    copy_args = {
        'table_name': DESTINATION_TABLE,
        's3_key': s3_key,
        'compression': 'GZIP'
    }
    redshift_loader.apply_async((copy_args, context))

    # Log success message to slack
    slack_message(
        f'{app.config["SUCCESS_ICON"]} Tagged {len(tagged_products)} new Shopify products!'
    )


def transform_product_tags(tagged_products: list, new_products: list) -> list:
    for tp, np in zip(tagged_products, new_products):
        # Rename widget_id > variant_id
        tp['variant_id'] = tp.pop('widget_id')

        # Add parent IDs to tagged products
        tp['parent_id'] = np['parent_id']

        # Drop classification features not stored in Redshift
        del tp['brand']
        del tp['name']

    return tagged_products


def get_new_products(count: int) -> dict:
    '''
    Selects untagged products from the shopify_product_listing table
    and returns data for classification in the product widget format.
    '''
    query = db.session.query(
        ShopifyProductListing.variant_id,
        ShopifyProductListing.parent_id,
        ShopifyProductListing.parent_title.label('name')
    ).outerjoin(
        ShopifyProductTags,
        ShopifyProductListing.variant_id == ShopifyProductTags.variant_id
    ).filter(
        # pylint: disable=singleton-comparison
        ShopifyProductTags.vertical == None
    ).limit(
        count
    )

    res = query.all()

    # Convert data to product widget format for
    # compatibility with the utils.product_classifier module
    # NOTE: We remove the color from the product name by spliting on ~
    # because the color names can cause problems for the classifier
    return [{
        'fields': {
            'brand': 'Sample Brand Name',
            'name': name.split('~')[0].strip()
        },
        'id': vid,
        'parent_id': pid
    } for vid, pid, name in res]


def upload_data(data: bytes) -> str:
    '''
    Uploads product tag data to S3.
    '''
    now = dt.datetime.now()
    key = f"transformed-data/sample/path/" \
          f"year={now.year}/month={now.month}/day={now.day}/" \
          f"sample_table_{now}.json.gz"

    app.s3_client.put_object(
        Bucket=app.config['S3_BUCKET'],
        Key=key,
        Body=data,
        ContentType='application/json'
    )

    return key
