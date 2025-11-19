'''
Sample script to extract transaction data from an affiliate network API
'''

import datetime as dt
from copy import deepcopy
from typing import List

from flask import current_app as app
from googleapiclient.discovery import Resource

from sample_framework import IterativePipeline, Pipeline
from sample_framework.steps import PipelineStep, RedshiftCopy, S3Upload
from sample_framework.utils import affiliate_data_transform as adt
from sample_framework.clients import create_google_api_client
from sample_framework.logging import slack_message
from sample_framework.requests import request_with_retries

from . import config, default_arg_sets

CRED_NAME = 'sample_network_credentials'
AFF_NETWORK = 'sample-network'
CREDENTIALS = app.config['CREDENTIALS'].get(CRED_NAME)
TABLE_MAP = {
    'US_transactions': 'sample_us_transaction_table',
    'UK_transactions': 'sample_uk_transaction_table',
    'US_products': 'sample_us_product_table',
    'UK_products': 'sample_uk_product_table'
}

COLUMN_MAP = {
    "transactions": {
        "commission_amount": "",
        "sale_amount": "",
        "product_count": "",
        "report_date": "validationDate",
        "purchase_date": "transactionDate",
        "click_date": "clickDate",
        "transaction_id": "id",
        "click_id": "",
        "referring_url": "publisherUrl",
        "referring_article": "",
        "referring_domain": "",
        "retailer": "",
        "raw_retailer": "",
        "affiliate_network": "",
        "report_year": "",
        "report_month": "",
        "report_day": "",
        "purchase_year": "",
        "purchase_month": "",
        "purchase_day": "",
        "click_year": "",
        "click_month": "",
        "click_day": "",
        "currency": ""
    },
    "products": {
        "product_name": "productName",
        "product_id": "skuCode",
        "network_product_id": "productId",
        "price": "unitPrice",
        "category": "category",
        "quantity": "quantity",
        "transaction_id": "",
        "brand": "",
        "retailer": "",
        "raw_retailer": "",
        "affiliate_network": "",
        "report_date": "",
        "report_year": "",
        "report_month": "",
        "report_day": "",
        "purchase_date": "",
        "purchase_year": "",
        "purchase_month": "",
        "purchase_day": "",
        "click_date": "",
        "click_year": "",
        "click_month": "",
        "click_day": "",
        "click_id": "",
        "referring_url": "",
        "referring_article": "",
        "referring_domain": "",
        "ir_campaign": "",
        "category_2": "",
        "category_3": "",
        "category_4": "",
        "category_5": ""
    }
}

SERVICE_NAME = 'sheets'
VERSION = 'v4'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEETID = 'sample_spreadsheet_id'
DATA_RANGE = 'sample_data_range'


def get_user_id(country: str) -> str:
    '''
    Get the user ID from credentials
    '''
    user_id_key = '{0}_USER_ID'.format(country)
    return CREDENTIALS.get(user_id_key)


def get_access_token(country: str) -> str:
    '''
    Get the access token from credentials
    '''
    token_key = '{0}_ACCESS_TOKEN'.format(country)
    return CREDENTIALS.get(token_key)


class Init(PipelineStep):
    def run(self, report_date: str, country: str) -> dict:
        '''
        Setting up variables for Pipeline framework
        '''
        extract_date = dt.datetime.strptime(report_date, '%Y-%m-%d')
        start = extract_date.strftime('%Y-%m-%dT00:00:00')
        end = extract_date.strftime('%Y-%m-%dT23:59:59')

        # Closed retailer accounts cause the program
        # info endpoint to return null so their ids are
        # stored here until all old transactions have posted
        sheets = self._get_sheets_service()
        retailer_map = self._get_closed_retailers(sheets)

        # Convert country into upper cased
        if country:
            if country.upper() not in ['US', 'UK']:
                raise AttributeError('Country should be US or UK.')
            country = country.upper()

        self.log_message(
            f'starting extraction for {country}: {start} to {end}')

        return {
            'start': start,
            'end': end,
            'country': country,
            'retailer_map': retailer_map
        }

    def _get_sheets_service(self):
        '''
        Create a Google Sheets client and return the .spreadsheets().values() service
        '''
        self.log_message('creating google sheets client')
        client = create_google_api_client(
            service_name=SERVICE_NAME,
            version=VERSION,
            oauth_file=app.config['GOOGLE_OAUTH_TOKEN']
        )
        return client.spreadsheets().values()

    def _get_closed_retailers(self, service: Resource) -> dict:
        '''
        Retrieve list of closed retailers from Google Sheets
        '''
        self.log_message('getting names from Google Sheets')
        response = service.get(
            spreadsheetId=SPREADSHEETID,
            range=DATA_RANGE
        ).execute()

        result = response.get('values', [])

        if result:
            fields = result.pop(0)
            retailers = {row[0]: row[1] for row in result}
            return retailers


class Extract(PipelineStep):
    def run(self, start: str, end: str, country: str) -> dict:
        '''
        Extract data from affiliate network API
        '''
        # Base api url for transaction list
        transaction_url = 'https://api.sample-network.com/publishers/{0}/transactions/'

        # Build request url
        user_id = get_user_id(country)
        request_url = transaction_url.format(user_id)

        params = {
            'startDate': start,
            'endDate': end,
            'timezone': 'UTC',
            'dateType': 'validation',
            'showBasketProducts': 'true',
            'accessToken': get_access_token(country)
        }

        # Send request
        self.log_message('sending request...')
        response = request_with_retries('GET', request_url, params=params)
        if response.status_code != 200:
            response.raise_for_status()

        return {
            'raw_extract': response.json()
        }


class Transform(PipelineStep):
    '''
    Transform data for Redshift
    '''
    def run(self, country: str, raw_extract: dict, retailer_map: list) -> dict:
        '''
        Setting up variables for Pipeline
        '''
        self.log_message('running transform')
        existing_transactions = adt.get_existing_transactions(
            raw_extract, AFF_NETWORK.replace('-', ' '), country)
        processed = self._process_transaction_status(
            raw_extract, existing_transactions, country)

        # Transform data and prepare formatting for S3Upload step
        reports = self._transform_data(processed, country, retailer_map)
        raw_data = {f'{AFF_NETWORK}_{country}_transactions': raw_extract}
        transformed_data = [
            dict(
                data=records,
                destination_table=TABLE_MAP[f'{country}_{report_type}'],
                file_name=f'{AFF_NETWORK}_{country}_{report_type}',
                report_type=report_type
            )
            for report_type, records in reports.items()
        ]
        return {
            'transformed_data': transformed_data,
            'raw_data': raw_data
        }

    def _transform_data(self, data: list, country: str, retailer_map: dict) -> dict:
        '''
        Standardizing data for affiliate data tables
        '''
        self.log_message('transforming data...')
        transformed_transactions = []
        transformed_products = []
        for entry in data:
            transaction = dict((key, None)
                               for key in COLUMN_MAP['transactions'])
            for field in transaction:
                if COLUMN_MAP['transactions'][field]:
                    transaction[field] = entry[COLUMN_MAP['transactions'][field]]
            formatted_transaction = self._format_transaction(
                transaction, entry, country, retailer_map)
            transformed_transactions.append(formatted_transaction)
            if entry['basketProducts']:
                # product entries are under basketProducts as a list of dicts
                for prod_entry in entry['basketProducts']:
                    product = dict((key, None)
                                   for key in COLUMN_MAP['products'])
                    for field in product:
                        if field in formatted_transaction:
                            product[field] = formatted_transaction[field]
                    formatted_product = self._format_product(
                        product, prod_entry)
                    transformed_products.append(formatted_product)
        reports = {'transactions': adt.fill_none(transformed_transactions),
                   'products': adt.fill_none(transformed_products)}
        return reports

    def _process_transaction_status(self, new_records: list, existing_records: list, country: str) -> List[dict]:
        '''
        This network only provides the latest snapshot of data, so we have to recreate
        the original transaction records when we receive records with returns.
        '''
        self.log_message('creating original transactions for returns')
        output_records = list()
        for new_record in new_records:
            if str(new_record['id']) in existing_records:
                adt.delete_transaction_record(
                    new_record['id'], AFF_NETWORK.replace('-', ' '), country)
                if new_record['basketProducts']:
                    for product in new_record['basketProducts']:
                        adt.delete_product_record(
                            new_record['id'], product['productId'], AFF_NETWORK.replace('-', ' '), country)
            if new_record['commissionStatus'] == 'declined':
                return_record = deepcopy(new_record)
                return_record['saleAmount']['amount'] = new_record['saleAmount']['amount'] * -1
                return_record['commissionAmount']['amount'] = new_record['commissionAmount']['amount'] * -1
                if return_record['basketProducts']:
                    for product_record in return_record['basketProducts']:
                        product_record['quantity'] *= -1
                output_records.append(new_record)
                output_records.append(return_record)

            elif (
                new_record['commissionStatus'] == 'approved' and
                new_record['amended'] and
                new_record['oldSaleAmount']['amount'] != new_record['saleAmount']['amount']
            ):
                # Create record for original amount
                full_record = deepcopy(new_record)
                full_record['saleAmount']['amount'] = new_record['oldSaleAmount']['amount']
                full_record['commissionAmount']['amount'] = new_record['oldCommissionAmount']['amount']

                # Create record for return amount
                return_record = deepcopy(new_record)
                return_sales = -1 * \
                    (full_record['saleAmount']['amount'] -
                     new_record['saleAmount']['amount'])
                return_commission = -1 * \
                    (full_record['commissionAmount']['amount'] -
                     new_record['commissionAmount']['amount'])
                return_record['saleAmount']['amount'] = return_sales
                return_record['commissionAmount']['amount'] = return_commission
                if return_record['basketProducts']:
                    for product_record in return_record['basketProducts']:
                        product_record['quantity'] *= -1

                output_records.append(full_record)
                output_records.append(return_record)

            else:
                output_records.append(new_record)
        return output_records

    def _format_transaction(self, transaction: dict, entry: dict, country: str, retailer_map: list) -> dict:
        '''
        Format transaction entries for affiliate transaction tables
        '''
        retailer_id = str(entry['advertiserId'])
        transaction = adt.format_date(transaction)
        transaction['raw_retailer'] = adt.format_raw_retailer(
            self._get_retailer(retailer_id, country, retailer_map))
        transaction['retailer'] = adt.format_retailer(
            transaction['raw_retailer'])
        transaction['sale_amount'] = entry['saleAmount']['amount']
        transaction['commission_amount'] = entry['commissionAmount']['amount']
        transaction['currency'] = entry['saleAmount']['currency']
        transaction['referring_article'] = adt.get_canonical(
            transaction['referring_url'])
        transaction['referring_domain'] = adt.get_site(
            transaction['referring_url'])
        transaction['affiliate_network'] = 'sample affiliate network'
        if entry['clickRefs']:
            transaction['click_id'] = entry['clickRefs'].get('clickRef')
        return transaction

    def _format_product(self, product: dict, product_entry: dict) -> dict:
        '''
        Format product entries for affiliate product data table
        '''
        product['product_name'] = adt.format_product_detail(
            product_entry['productName'])
        product['product_id'] = product_entry['skuCode']
        product['network_product_id'] = product_entry['productId']
        product['price'] = product_entry['unitPrice']
        product['category'] = adt.format_product_detail(
            product_entry['category'])
        product['quantity'] = product_entry['quantity']
        return product

    def _get_retailer(self, advertiser_id: str, country: str, retailer_map: dict) -> str:
        '''
        Check retailer map for advertiser id
        '''
        if advertiser_id in retailer_map:
            return retailer_map[advertiser_id]
        else:
            # Base api url for advertiser details
            advertiser_url = 'https://api.sample-network.com/publishers/{0}/programmedetails'

            user_id = get_user_id(country)
            request_url = advertiser_url.format(user_id)

            # Set request params
            params = {
                'advertiserId': int(advertiser_id),
                'accessToken': get_access_token(country)
            }

            # Send request
            response = request_with_retries('GET', request_url, params=params)
            if response.status_code != 200:
                response.raise_for_status()

            json_response = response.json()

            # Grab retailer name
            try:
                raw_retailer_name = json_response['programmeInfo']['name']
            except (TypeError, KeyError) as e:
                raise KeyError(
                    'no retailer info for advertiser ID: {0}'.format(advertiser_id))

            retailer_map[advertiser_id] = raw_retailer_name.lower()
            return raw_retailer_name.lower()


class SlackMessage(PipelineStep):
    def run(self, country: str, transformed_data: dict) -> None:
        '''
        Print the number of records extracted for each report type
        '''
        message_data = [
            dict(
                report_type=report.get('report_type'),
                record_count=len(report.get('data'))
            )
            for report in transformed_data
        ]
        # Sort message components by reverse order to print transactions first
        sorted(message_data, key=lambda x: x['report_type'], reverse=True)
        for message in message_data:
            slack_message(
                f"{app.config['SUCCESS_ICON']} Pulled Data from Sample Network"
                f"{country} {message['report_type']}! Total: {message['record_count']}"
            )


iteration = Pipeline(
    **config.to_dict(),
    steps=[
        Init('init'),
        Extract('extract'),
        Transform('transform'),
        S3Upload(
            step_name='s3_upload',
            category=config.category.lower(),
            package_name=config.package_name,
            raw_data_type='application/json'
        ),
        RedshiftCopy('load'),
        SlackMessage('slack_message')
    ]
)

pipe = IterativePipeline(
    default_arg_sets=default_arg_sets,
    pipeline=iteration,
    stop_on_error=True
)
