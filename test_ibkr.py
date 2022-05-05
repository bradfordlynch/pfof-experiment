import pickle
import threading
import time

from trading import IBAccount
from ibapi.contract import Contract
from ibapi.order import *
from utils import PrintLogger


def run_loop():
    app.run()


def Stock_contract(symbol, secType="STK", exchange="SMART", currency="USD"):
    """custom function to create stock contract"""
    contract = Contract()
    contract.symbol = symbol
    contract.secType = secType
    contract.exchange = exchange
    contract.currency = currency
    return contract


if __name__ == "__main__":
    app = IBAccount(PrintLogger())
    app.connect("127.0.0.1", 7496, 124)

    app.nextorderId = None

    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    # Check if the API is connected via orderid
    while True:
        if isinstance(app.nextorderId, int):
            print("connected")
            break
        else:
            print("waiting for connection")
            time.sleep(1)

    # Create contract object
    apple_contract = Stock_contract("AAPL")

    # Create order object
    order = Order()
    order.action = "BUY"
    order.totalQuantity = 1
    order.orderType = "LMT"
    order.lmtPrice = "150"

    # Request Market Data
    # _start = time.time()
    # app.placeOrder(app.nextorderId, apple_contract, order)
    # print(f"Took {(time.time() - _start) * 1000:.2f} ms")
    # time.sleep(3)
    # _start = time.time()
    # app.cancelOrder(app.nextorderId)
    # print(f"Took {(time.time() - _start) * 1000:.2f} ms")
    _start = time.time()
    # app.get_executions("AAPL")
    buy_order = app.buy_limit("AAPL", 1, 159)
    # buy_order = app.buy_market("AAPL", 1)
    print(buy_order)
    # print(buy_order)
    print(f"Took {(time.time() - _start) * 1000:.2f} ms")
    time.sleep(1)

    order = app.get_order_info(buy_order.order_id)
    print(order)

    _start = time.time()
    # sell_order = app.sell_limit("AAPL", 1, 159.5)
    cancelled = app.cancel_order(buy_order.order_id)
    print("State from cancel function:")
    print(cancelled)
    print(f"Took {(time.time() - _start) * 1000:.2f} ms")
    # time.sleep(1)

    # order = app.get_order_info(buy_order.order_id)
    # print("After cancelling:")
    # print(order)

    # sell_order_updated = app.get_order_info(sell_order.order_id)

    # time.sleep(1)  # Sleep interval to allow time for incoming price data

    # with open("ibkr_test_output.pkl", "wb") as out_file:
    #     pickle.dump(
    #         {
    #             "buy_resp": buy_order,
    #             "buy_order": order,
    #             "sell_resp": sell_order,
    #             "sell_order": sell_order_updated,
    #         },
    #         out_file,
    #     )

    app.disconnect()
