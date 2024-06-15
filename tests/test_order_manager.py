import unittest
import pandas as pd
from main import OrderManager

class TestOrderManager(unittest.TestCase):

    def setUp(self):
        self.orders_data = {
            'Pair': ['JTOUSDT', 'JTOUSDT'],
            'Direction': ['BUY', 'SELL'],
            'Price': [2.00012345, 2.00198765],
            'Quantity': [3.7224567, 3.0671234],
            'Account': [1, 2],
            'Value': [7.44, 6.14]
        }
        self.orders_df = pd.DataFrame(self.orders_data)

        self.precision_data = {
            'Account': [1, 2],
            'Price Precision': [4, 2],
            'Quantity Precision': [1, 3]
        }
        self.precision_df = pd.DataFrame(self.precision_data)

        self.manager = OrderManager(rate_limit=10, dry_run=True)
        self.manager.orders = self.orders_df
        self.manager.precision = self.precision_df

    def test_validate_orders(self):
        self.manager.validate_orders()

        self.assertEqual(self.manager.orders.at[0, 'Price'], 2.0001)
        self.assertEqual(self.manager.orders.at[0, 'Quantity'], 3.7)
        self.assertEqual(self.manager.orders.at[1, 'Price'], 2.00)
        self.assertEqual(self.manager.orders.at[1, 'Quantity'], 3.067)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
