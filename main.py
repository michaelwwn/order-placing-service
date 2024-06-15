import asyncio
import aiohttp
import pandas as pd
import os
from dotenv import load_dotenv
import logging
from typing import Dict
import hmac
import hashlib
import time

from error_codes import ERR_PRICE_EXCEEDS_THRESHOLD, ERR_INSUFFICIENT_BALANCE

# Load environment variables
load_dotenv()

BASE_URL = os.getenv('BASE_URL', 'http://localhost:8081')
ORDERS_CSV_PATH = os.getenv('ORDERS_CSV_PATH', 'data/Orders.csv')
PRECISION_CSV_PATH = os.getenv('PRECISION_CSV_PATH', 'data/Precision.csv')
RATE_LIMIT = int(os.getenv('RATE_LIMIT', 10))
BACKOFF_FACTOR = int(os.getenv('BACKOFF_FACTOR', 2))
RETRY_ATTEMPTS = int(os.getenv('RETRY_ATTEMPTS', 3))
DRY_RUN = os.getenv('DRY_RUN', 'True').strip().lower() in {'true', '1', 't'}

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class ExchangeClient:
    def __init__(self, api_key, api_secret, account):
        """
        Initializes the ExchangeClient with API credentials and account details.

        :param api_key: API key for authentication
        :param api_secret: API secret for authentication
        :param account: Account identifier
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.account = account
        self.base_url = BASE_URL
        self.session = aiohttp.ClientSession(headers={
            'X-APP-APIKEY': self.api_key
        })

    def _generate_signature(self, data):
        """
        Generates HMAC SHA256 signature.

        :param data: Data to be signed
        :return: HMAC SHA256 signature
        """
        query_string = '&'.join(
            [f"{key}={value}" for key, value in data.items()])
        signature = hmac.new(self.api_secret.encode(
            'utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
        return signature

    async def close(self):
        """
        Closes the aiohttp session.

        :return: None
        """
        await self.session.close()

    async def place_order(self, order):
        """
        Places an order on the exchange.

        :param order: Dictionary containing order details
        :return: JSON response from the exchange if the order is placed successfully
        :raises: Exception if the order placement fails
        """
        url = f"{self.base_url}/v1/order"

        payload = {
            'pair': order['Pair'],
            'type': order['Direction'].lower(),
            'price': order['Price'],
            'quantity': order['Quantity'],
            'account': self.account
        }

        # Generate the signature
        payload['signature'] = self._generate_signature(payload)

        async with self.session.post(url, json=payload) as response:
            data = await response.json()
            if response.status == 200:
                return data
            else:
                error_code = data.get('code')
                if error_code == ERR_INSUFFICIENT_BALANCE:
                    raise Exception("Insufficient balance")
                elif error_code == ERR_PRICE_EXCEEDS_THRESHOLD:
                    raise Exception("Price or quantity exceeds the threshold")
                else:
                    raise Exception(
                        f"Error from Exchange: {data.get('msg', 'Unknown error')}")

    async def get_account(self):
        """
        Fetches the account balance from the exchange.

        :return: Dictionary containing account balances
        :raises: Exception if the request fails
        """
        url = f"{self.base_url}/v1/account"

        payload = {
            'timestamp': int(time.time() * 1000)
        }

        payload['signature'] = self._generate_signature(payload)

        query_string = '&'.join(
            [f"{key}={value}" for key, value in payload.items()])
        request_url = f"{url}?{query_string}"

        async with self.session.get(request_url) as response:
            if response.status == 200:
                return await response.json()
            else:
                raise Exception(f"Failed to fetch balance: {response.status}")


class OrderManager:
    def __init__(self, rate_limit, dry_run=False):
        """
        Initializes the OrderManager with rate limit and dry-run mode.

        :param rate_limit: Maximum number of requests per second
        :param dry_run: Boolean indicating if dry-run mode is enabled
        """
        self.clients: Dict[str, ExchangeClient] = {}
        self.rate_limit = rate_limit
        self.dry_run = dry_run
        self.orders = []
        self.successful_orders = set()

    def load_orders(self, orders_csv, precision_csv):
        """
        Loads orders and market metadata from CSV files.

        :param orders_csv: Path to the CSV file containing orders
        :param precision_csv: Path to the CSV file containing precision data
        :return: None
        """
        self.orders = pd.read_csv(orders_csv)
        self.precision = pd.read_csv(precision_csv)

    def load_accounts(self):
        """
        Loads account credentials from environment variables and initializes ExchangeClients.

        :return: None
        """
        valid_accounts = []

        # Check each account in precision data for credentials
        for index, row in self.precision.iterrows():
            account_id = row['Account']
            key_var = f"API_KEY_{index+1}"
            secret_var = f"API_SECRET_{index+1}"
            account_var = f"API_ACCOUNT_{index+1}"

            if key_var in os.environ and secret_var in os.environ and account_var in os.environ:
                self.clients[account_id] = ExchangeClient(
                    api_key=os.environ[key_var],
                    api_secret=os.environ[secret_var],
                    account=os.environ[account_var]
                )
                valid_accounts.append(account_id)
            else:
                logging.warning(
                    f"Missing credentials for account: {account_id}")

        if not valid_accounts:
            raise Exception("No valid accounts with credentials found.")

    async def place_order_with_retry(self, index, order):
        """
        Attempts to place an order with retry logic to prevent duplicate entries.
        Exponential backoff is used to space out retry attempts.

        :param index: Index of the order in the DataFrame
        :param order: Dictionary containing order details
        :return: None
        :raises: Exception if an error occurs during order placement
        """
        client = self.clients[order['Account']]
        retry_attempts = RETRY_ATTEMPTS

        for attempt in range(retry_attempts):
            try:
                response = await client.place_order(order)
                logging.info(f"Order placed: {response}")
                self.successful_orders.add(index)
                break
            except Exception as e:
                wait_time = BACKOFF_FACTOR ** attempt
                logging.error(
                    f"Attempt {attempt + 1} failed for order {index}: {e}. "
                    f"Retrying in {wait_time} seconds.")
                if attempt < retry_attempts - 1:
                    await asyncio.sleep(wait_time)
                else:
                    logging.error(
                        f"Order {index} failed after {retry_attempts} attempts.")
                    raise e

    async def place_orders(self):
        """
        Places orders on the exchange, adhering to the rate limit and retry logic.

        :return: None
        :raises: Exception if an error occurs during order placement
        """
        for index, order in self.orders.iterrows():
            if index in self.successful_orders:
                continue
            try:
                if self.dry_run:
                    logging.info(f"Dry run: Validating order: {order}")
                else:
                    await self.place_order_with_retry(index, order)
                await asyncio.sleep(1 / self.rate_limit)
            except Exception as e:
                logging.info(f"Error placing order: {e}")
                break

    async def close_sessions(self):
        """
        Closes the session for each client.

        :return: None
        """
        for client in self.clients.values():
            await client.close()

    def validate_orders(self):
        """
        Validates the loaded orders. This includes checking balances and order limits.

        :return: None
        :raises: Exception if an error occurs during validation
        """
        # Check for missing columns
        required_columns = {'Pair', 'Direction',
                            'Price', 'Quantity', 'Account'}
        if not required_columns.issubset(self.orders.columns):
            missing_cols = required_columns - set(self.orders.columns)
            raise Exception(f"Missing columns: {', '.join(missing_cols)}")

        # Check for invalid values
        for index, order in self.orders.iterrows():
            if order['Direction'] not in ['BUY', 'SELL']:
                raise Exception(
                    f"Row {index}: Invalid direction {order['Direction']}")
            if order['Price'] < 0:
                raise Exception(f"Row {index}: Price cannot be negative")
            if order['Quantity'] <= 0:
                raise Exception(
                    f"Row {index}: Quantity must be greater than zero")
            if order['Account'] not in self.precision['Account'].values:
                raise Exception(
                    f"Row {index}: Account {order['Account']} not found in precision data")

            # Validate price and quantity precision
            precision = self.precision[self.precision['Account']
                                       == order['Account']].iloc[0]

            price_precision_format = f"{{:.{precision['Price Precision']}f}}"
            quantity_precision_format = f"{{:.{precision['Quantity Precision']}f}}"

            self.orders.at[index, 'Price'] = float(
                price_precision_format.format(order['Price']))
            self.orders.at[index, 'Quantity'] = float(
                quantity_precision_format.format(order['Quantity']))

    async def validate_api_credentials(self):
        """
        Validates the API credentials by attempting to fetch the account balance.

        :return: None
        :raises: Exception if the API credentials are invalid
        """
        for _, row in self.precision.iterrows():
            client = self.clients[row['Account']]
            try:
                await client.get_account()
                logging.info(
                    f"Account {row['Account']} validated successfully")
            except Exception as e:
                raise Exception(f"API credential validation failed: {e}")

    async def execute(self):
        """
        Executes the order placement process. If dry-run mode is enabled, only validates orders.

        :return: None
        :raises: Exception if an error occurs during execution
        """
        try:
            self.validate_orders()
            await self.validate_api_credentials()
            await self.place_orders()
        except Exception as e:
            print(f"Execution terminated due to error: {e}")


async def order_listing_start():
    """
    Executes the order placement process for a single account.

    :return: None
    :raises: Exception if an error occurs during execution
    """
    try:
        manager = OrderManager(RATE_LIMIT, DRY_RUN)
        manager.load_orders(ORDERS_CSV_PATH, PRECISION_CSV_PATH)
        manager.load_accounts()
        await manager.execute()
    finally:
        await manager.close_sessions()


if __name__ == "__main__":
    asyncio.run(order_listing_start())
