import ccxt
from ccxt import exchanges
import time
from datetime import datetime
from typing import Annotated

def request_ohlcv(exchange, pair_id) -> Annotated[any, 'any']:
    response = exchange.fetch_ohlcv(pair_id, timeframe='1m', limit=5)
    return response



def get_api() -> Annotated[list[tuple[any]], 'data binance']:
    exchange = ccxt.binance ({
        'rateLimit': 1,  # unified exchange property
        'headers': {
            'YOUR_CUSTOM_HTTP_HEADER': 'YOUR_CUSTOM_VALUE',
        },
        'options': {
            'adjustForTimeDifference': True,  # exchange-specific option
        }
    })

    start = time.time()
    pair_id = 'NEAR/USDT'
    responses = request_ohlcv(exchange, pair_id)
    # print(time.time() - start)
    a = []
    for response in responses:
        response = [str(index) for index in response]
        # insert_data(response)
        response[0] = str(datetime.fromtimestamp(float(response[0])/1000))
        response.insert(0, pair_id)
        # print(tuple(response))
        a.append(tuple(response))
    # print(time.time() - start)
    return a




