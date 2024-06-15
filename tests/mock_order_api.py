from aiohttp import web
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

accounts = {
    "DEMO_API_KEY_1": {"balance": 10000, "email": "account1@example.com", "secret": "API_SECRET_1"},
    "DEMO_API_KEY_2": {"balance": 15000, "email": "account2@example.com", "secret": "API_SECRET_2"},
    "DEMO_API_KEY_3": {"balance": 12000, "email": "account3@example.com", "secret": "API_SECRET_3"}
}


async def authenticate_request():
    """
    Mock authentication function.

    :return: Always returns True for simplicity
    """
    return True


async def handle_place_order(request):
    """
    Handles the placement of an order. Authenticates the request, checks account balance,
    and places the order if valid.

    :param request: HTTP request object
    :return: JSON response indicating success or failure of the order placement
    """
    if not await authenticate_request():
        return web.json_response({"message": "Authentication failed"}, status=401)

    api_key = request.headers.get('X-APP-APIKEY')
    payload = await request.json()
    account_balance = accounts[api_key]['balance']
    order_amount = float(payload['price']) * float(payload['quantity'])
    
    logging.info(
        f"Order Fill - Pair: {payload['pair']}, Price {payload['price']}, Quantity: {payload['quantity']}, Cash Value: {order_amount}, Email: {accounts[api_key]['email']}")

    if order_amount > account_balance:
        return web.json_response({
            "code": -2001,
            "msg": "Insufficient balance"
        }, status=400)
    elif float(payload['price']) > 100:
        return web.json_response({
            "code": -2002,
            "msg": "Price exceeds the threshold"
        }, status=400)

    accounts[api_key]['balance'] -= order_amount
    return web.json_response({"message": "Order placed successfully", "new_balance": accounts[api_key]['balance']})


async def handle_get_account(request):
    """
    Handles the retrieval of account information. Authenticates the request
    and returns account details.

    :param request: HTTP request object
    :return: JSON response containing account details
    """
    if not await authenticate_request():
        return web.json_response({"message": "Authentication failed"}, status=401)

    api_key = request.headers.get('X-APP-APIKEY')

    return web.json_response(accounts[api_key])

app = web.Application()
app.add_routes([
    web.post('/v1/order', handle_place_order),
    web.get('/v1/account', handle_get_account),
])

if __name__ == '__main__':
    web.run_app(app, port=8000)
