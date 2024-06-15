# Order Listing Service

## Introduction
Order Listing Service is use to place list of orders as soon as possible on the exchange spot market

## Assumption
1. Each account has a unique API Key.
2. Orders will be filled immediately regardless of the price.
3. The mock server's balance is in USDT.
4. The mock server can be accessed without authentication.
5. The mock server only support BUY.

## Installation
```bash
# Install Dependencies
python3 -m pip install -r requirements.txt
# Copy Environment File
cp .env.example .env
```

### Running the server
The mock server will run on port 8000. To start the mock server, run:
```bash
python3 tests/mock_order_api.py
```

### Running Locally
```bash
python3 main.py
```

### Running the test
```bash
python3 -m unittest discover -s tests
```