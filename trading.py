from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from functools import partial
import os
import secrets
import time
import threading
from typing import Callable

from requests import Session
import pyotp
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order as IBOrder
from ibapi.execution import ExecutionFilter
import robin_stocks.robinhood as rh_client
import tda
from tda import auth
from tda.orders.equities import (
    equity_buy_limit,
    equity_buy_market,
    equity_sell_limit,
    equity_sell_market,
)
from tda.orders.common import Duration
from tda.orders.common import Session as TDASession
from utils import get_secret, put_secret


@dataclass
class Order:
    """
    Class for keeping track of the state of an order
    """

    order_id: str
    t_created: str
    symbol: str
    side: str
    state: str
    filled: bool
    avg_price: float
    quantity: int
    cumulative_quantity: int
    executions: list
    events: list

    def dollar_volume(self):
        return self.avg_price * self.quantity


class Broker(ABC):
    """
    Base class for all brokers. Wrap broker-specific business logic inside a common API.
    """

    def __init__(self, logger) -> None:
        super().__init__()
        self.logger = logger

    @abstractmethod
    def buy_market(self, symbol: str, quantity: int) -> Order:
        """
        Submits a market order to buy `quantity` shares of `symbol` at any price

        Args:
        symbol (`str`):
            The trading symbol associated with the asset
        quantity (`int`):
            The number of shares to purchase
        """
        raise NotImplementedError

    @abstractmethod
    def buy_limit(self, symbol: str, quantity: int, limit_price: float) -> Order:
        """
        Submits a limit order to buy `quantity` shares of `symbol` for no more than `limit_price`

        Args:
        symbol (`str`):
            The trading symbol associated with the asset
        quantity (`int`):
            The number of shares to purchase
        limit_price (`float`):
            The maximum price to pay for the asset
        """
        raise NotImplementedError

    @abstractmethod
    def sell_market(self, symbol: str, quantity: int) -> Order:
        """
        Submits a market order to sell `quantity` shares of `symbol` at any price

        Args:
        symbol (`str`):
            The trading symbol associated with the asset
        quantity (`int`):
            The number of shares to sell
        """
        raise NotImplementedError

    @abstractmethod
    def sell_limit(self, symbol: str, quantity: int, limit_price: float) -> Order:
        """
        Submits a limit order to sell `quantity` shares of `symbol` for no less than `limit_price`

        Args:
        symbol (`str`):
            The trading symbol associated with the asset
        quantity (`int`):
            The number of shares to sell
        limit_price (`float`):
            The minimum price to recieve for the asset
        """
        raise NotImplementedError

    @abstractmethod
    def get_order_info(self, order_id: str) -> Order:
        """
        Retrieves the latest state of the order associated with `order_id`

        Args:
        order_id (`str`):
            The order_id associated with the order
        """
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, order_id: str) -> Order:
        """
        Cancels the order associated with `order_id` if it has not already filled

         Args:
         order_id (`str`):
             The order_id associated with the order
        """
        raise NotImplementedError

    @abstractmethod
    def cleanup(self) -> None:
        """
        Do anything that needs to be done when the account is shut down.
        """
        raise NotImplementedError


class IBAccount(Broker, EWrapper, EClient):
    """
    Live trading account at Interactive Broker

    Args:
    logger:
        logging.Logger like object implementing debug, info, warn, and error methods
    """

    def __init__(self, logger) -> None:
        EClient.__init__(self, self)

        self.logger = logger
        self.orders = dict()

        def run_loop():
            self.run()

        self.connect("127.0.0.1", 7496, 124)

        self.nextorderId = None

        # Start the socket in a thread
        self.api_thread = threading.Thread(target=run_loop, daemon=True)
        self.api_thread.start()

        # Check if the API is connected via orderid
        while True:
            if isinstance(self.nextorderId, int):
                logger.info("IBKR - Connected")
                break
            else:
                logger.info("IBKR - Waiting for connection")
                time.sleep(1)

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextorderId = orderId
        print("The next valid order id is: ", self.nextorderId)

    def orderStatus(
        self,
        orderId,
        status,
        filled,
        remaining,
        avgFillPrice,
        permId,
        parentId,
        lastFillPrice,
        clientId,
        whyHeld,
        mktCapPrice,
    ) -> None:
        """
        Processes orderStatus events from the TWS API.
        """
        ts_event = time.time_ns()
        try:
            # Ensure that there is a record of the order in the IBAccount state,
            # otherwise it might be an event associated with an extraneous order
            # placed via the GUI or another TWS session.
            assert orderId in self.orders
            # Append the event to the order
            event = {
                "type": "orderStatus",
                "ts": ts_event,
                "order_id": orderId,
                "status": status,
                "q_filled": filled,
                "q_remaining": remaining,
                "avg_price": avgFillPrice,
            }
            self.orders[orderId].events.append(event)

            # Update Order attributes with new data
            if status == "Filled":
                self.orders[orderId].state = "filled"
                self.orders[orderId].filled = True
            elif status == "Cancelled":
                self.orders[orderId].state = "cancelled"
            self.orders[orderId].avg_price = avgFillPrice
            self.orders[orderId].cumulative_quantity = filled
            self.logger.info(event)
        except AssertionError:
            self.logger.error(
                f"IBKR - Got orderStatus event for orderId {orderId} which does not exist"
            )

    def openOrder(self, orderId, contract, order, orderState) -> None:
        """
        Processes openOrder events from the TWS API. Most orders lead to multiple
        openOrder events, e.g., one when the order is first placed, another once
        it is filled, and another once TWS computes the commissions associated with
        the order.
        """
        ts_event = time.time_ns()
        try:
            assert orderId in self.orders
            # Append the event to the order
            event = {
                "type": "openOrder",
                "ts": ts_event,
                "order_id": orderId,
                "contract": contract.__dict__,
                "order": order.__dict__,
                "order_state": orderState.__dict__,
            }
            self.orders[orderId].events.append(event)
            self.logger.info(event)
        except AssertionError:
            self.logger.error(
                f"IBKR - Got openOrder event for orderId {orderId} which does not exist"
            )

    def execDetails(self, reqId, contract, execution) -> None:
        """
        Processes execDetails events from the TWS API. Orders may lead to multiple
        execDetails events if the order is filled across venues or times.
        """
        ts_event = time.time_ns()
        orderId = execution.orderId
        try:
            assert orderId in self.orders
            # Append the event to the order
            event = {
                "type": "execDetails",
                "ts": ts_event,
                "order_id": orderId,
                "req_id": reqId,
                "contract": contract.__dict__,
                "execution": execution.__dict__,
            }
            self.orders[orderId].events.append(event)
            # Update Order attributes with new data
            self.orders[orderId].avg_price = execution.avgPrice
            self.orders[orderId].cumulative_quantity = execution.cumQty
            self.logger.info(event)
        except AssertionError:
            self.logger.error(
                f"IBKR - Got execDetails event for orderId {orderId} which does not exist"
            )

    def _get_stock_contract(
        self, symbol, secType="STK", exchange="SMART", currency="USD"
    ):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = secType
        contract.exchange = exchange
        contract.currency = currency
        return contract

    def buy_market(self, symbol: str, quantity: int) -> Order:
        return self._market_order(symbol, quantity, "BUY")

    def buy_limit(self, symbol: str, quantity: int, limit_price: float) -> Order:
        return self._limit_order(symbol, quantity, limit_price, "BUY")

    def sell_market(self, symbol: str, quantity: int) -> Order:
        return self._market_order(symbol, quantity, "SELL")

    def sell_limit(self, symbol: str, quantity: int, limit_price: float) -> Order:
        return self._limit_order(symbol, quantity, limit_price, "SELL")

    def get_order_info(self, order_id: str) -> Order:
        return self.orders[order_id]

    def cancel_order(self, order_id: str) -> Order:
        if self.orders[order_id].filled:
            # Order has been filled, no need to cancel
            return self.orders[order_id]
        else:
            # Cancel order
            self.cancelOrder(order_id)

            # Order update is async, wait up to 500ms for state to update
            ts_wait_start = time.time()
            order = self.orders[order_id]
            while order.state != "cancelled":
                order = self.orders[order_id]

                if time.time() - ts_wait_start >= 0.5:
                    self.logger.error(
                        f"IBKR - Timed out while waiting for order {order_id} cancelation"
                    )
                    break

            if order.state == "cancelled":
                self.logger.info(
                    f"IBKR - Waited {(time.time() - ts_wait_start) * 1000:.2f} ms for order {order_id} cancelation"
                )

            return order

    def cleanup(self):
        # Disconnect from TWS
        self.disconnect()

        # Wait or timeout for socket thread to shutdown
        self.api_thread.join(2.0)

    def get_executions(self, symbol: str):
        exec_filter = ExecutionFilter()
        exec_filter.symbol = symbol

        self.reqExecutions(0, exec_filter)

    def _market_order(self, symbol: str, quantity: int, direction: str) -> Order:
        contract = self._get_stock_contract(symbol)

        order = IBOrder()
        assert direction in ["BUY", "SELL"], f"Invalid direction {direction}"
        order.action = direction
        order.totalQuantity = quantity
        order.orderType = "MKT"
        order.tif = "GTC"
        order.outsideRth = True

        return self._place_order(contract, order)

    def _limit_order(
        self, symbol: str, quantity: int, limit_price: float, direction: str
    ) -> Order:
        contract = self._get_stock_contract(symbol)

        order = IBOrder()
        assert direction in ["BUY", "SELL"], f"Invalid direction {direction}"
        order.action = direction
        order.totalQuantity = quantity
        order.orderType = "LMT"
        order.lmtPrice = limit_price
        order.tif = "GTC"
        order.outsideRth = True

        return self._place_order(contract, order)

    def _place_order(self, contract: Contract, ib_order: IBOrder):
        order_id = self.nextorderId
        self.orders[order_id] = Order(
            order_id,
            time.time_ns(),
            contract.symbol,
            ib_order.action,
            "live",
            False,
            -1,
            ib_order.totalQuantity,
            0,
            [],
            [],
        )
        self.placeOrder(order_id, contract, ib_order)
        self.nextorderId += 1

        return self.orders[order_id]


class TDAAccount(Broker):
    """
    Live trading account at TDAmeritrade

    Args:
        logger:
            logging.Logger like object implementing debug, info, warn, and error methods
        id_secret (`str`):
            Name of the secret in AWS
        account_name:
            Name of the account as referenced in the secret
    """

    def __init__(self, logger, id_secret: str, account_name: str) -> None:
        self.logger = logger
        self.id_secret = id_secret
        self.account_name = account_name
        self.token_path = f"/tmp/tda_{account_name}_{secrets.token_urlsafe(8)}.json"
        self.api_key = "HAQOYO2PWGK3URPBXPUNZTMKEX0FZJC8@AMER.OAUTHAP"

        self.client, self.account_id = self._setup_client()

        self.orders = dict()

    def buy_market(self, symbol: str, quantity: int) -> Order:
        order = equity_buy_market(symbol, quantity).build()

        return self._place_order(order)

    def buy_limit(self, symbol: str, quantity: int, limit_price: float) -> Order:
        order = (
            equity_buy_limit(symbol, quantity, limit_price)
            .set_duration(Duration.GOOD_TILL_CANCEL)
            .set_session(TDASession.SEAMLESS)
            .build()
        )

        return self._place_order(order)

    def sell_market(self, symbol: str, quantity: int) -> Order:
        order = equity_sell_market(symbol, quantity).build()

        return self._place_order(order)

    def sell_limit(self, symbol: str, quantity: int, limit_price: float) -> Order:
        order = (
            equity_sell_limit(symbol, quantity, limit_price)
            .set_duration(Duration.GOOD_TILL_CANCEL)
            .set_session(TDASession.SEAMLESS)
            .build()
        )

        return self._place_order(order)

    def get_order_info(self, order_id: str) -> Order:
        # Get order info from TDA
        ts_event = time.time_ns()
        resp = self.client.get_order(order_id, self.account_id)
        try:
            assert resp.status_code == 200
        except Exception as e:
            self.logger.error(
                f"TDA - Failed to get order {order_id}. Received {resp.status_code}"
            )

            return self.orders[order_id]

        # Update order state
        order_state = resp.json()
        self.orders[order_id].filled = (
            order_state["quantity"] == order_state["filledQuantity"]
        )
        if self.orders[order_id].filled:
            self.orders[order_id].state = "filled"
        self.orders[order_id].cumulative_quantity = order_state["filledQuantity"]

        # TDA doesn't provide a VWAP, need to compute it ourselves
        if "orderActivityCollection" in order_state:
            self.orders[order_id].avg_price = self._compute_avg_price(
                order_state["orderActivityCollection"]
            )

        self.orders[order_id].events.append(
            {"type": "get_order_info", "ts": ts_event, "order_state": order_state}
        )

        return self.orders[order_id]

    def cancel_order(self, order_id: str) -> Order:
        # Get the current state of the order
        order = self.get_order_info(order_id)

        if order.filled:
            # Order filled, no need to cancel it
            return order
        else:
            # Order isn't filled, need to cancel it
            ts_cancel = time.time_ns()
            resp = self.client.cancel_order(order_id, self.account_id)
            cancelled = False
            try:
                assert resp.status_code == 200
                self.orders[order_id].state = "cancelled"
                cancelled = True
            except Exception as e:
                self.logger.error(
                    f"TDA - Failed to cancel order {order_id}. Received {resp.status_code}"
                )
                self.logger.error(f"TDA - {resp.text}")

            try:
                body = resp.json()
            except:
                body = resp.text

            self.orders[order_id].events.append(
                {"type": "cancel_order", "ts": ts_cancel, "resp": body}
            )

            return self.orders[order_id]

    def _place_order(self, tda_order) -> Order:
        """
        Submits a TDA order template for execution and converts the response into an
        Order object
        """
        ts_order_submit = time.time_ns()
        resp = self.client.place_order(self.account_id, tda_order)
        order_id = tda.utils.Utils(self.client, self.account_id).extract_order_id(resp)

        self.orders[order_id] = Order(
            order_id,
            ts_order_submit,
            tda_order["orderLegCollection"][0]["instrument"]["symbol"],
            tda_order["orderLegCollection"][0]["instruction"],
            "live",
            False,
            -1,
            tda_order["orderLegCollection"][0]["quantity"],
            0,
            [],
            [],
        )

        return self.orders[order_id]

    def _compute_avg_price(self, activity_collection):
        """
        Computes the avaerage price based on one or more executions.

        See TDA for schema:
        https://developer.tdameritrade.com/account-access/apis/get/accounts/%7BaccountId%7D/orders/%7BorderId%7D-0

        Args:
            activity_collection (`list`):
                List of events associated with the order
        """
        q = 0
        p = 0

        for event in activity_collection:
            if event["activityType"] == "EXECUTION":
                q += event["quantity"]
                p += event["quantity"] * event["executionLegs"][0]["price"]

        return p / q

    def _setup_client(self):
        secret = get_secret(self.id_secret)

        with open(self.token_path, "w") as out_file:
            out_file.write(secret[f"token_{self.account_name}"])

        try:
            client = auth.client_from_token_file(self.token_path, self.api_key)
        except FileNotFoundError:
            self.logger.error("TDA - Failed to load credentials from file")
            raise
        except Exception as e:
            self.logger.error(
                f"TDA - Unexpected {type(e)} error when setting up client"
            )
            self.logger.error(f"TDA - {e}")
            raise

        try:
            account_id = secret[f"account_{self.account_name}"]
        except KeyError:
            self.logger.error(
                f"TDA - Secret is missing account id for account named {self.account_name}"
            )
            raise
        except Exception as e:
            self.logger.error(
                f"TDA - Unexpected {type(e)} error when getting account id"
            )
            self.logger.error(f"TDA - {e}")
            raise

        return client, account_id

    def cleanup(self):
        """
        Handles anything that needs to be done once trading has ended
        for the day.
        """
        # TDA token may have been updated during operation,
        # update the secrets in AWS with the latest token
        secret = get_secret(self.id_secret)

        with open(self.token_path, "r") as in_file:
            token = in_file.read()

        secret[f"token_{self.account_name}"] = token

        put_secret(self.id_secret, secret)

        # Delete token from machine
        os.remove(self.token_path)


class RobinhoodAccount(Broker):
    """
    Live trading account at Robinhood

    Args:
        logger:
            logging.Logger like object implementing debug, info, warn, and error methods
        id_secret (`str`):
            Name of the secret in AWS
        account_name:
            Name of the account as referenced in the secret
    """

    def __init__(self, logger, id_secret: str, account_name: str) -> None:
        self.logger = logger
        self.id_secret = id_secret
        self.account_name = account_name

        # Authenticate the robin_stocks library with HOOD
        self._setup_client()

        self.orders = dict()

    def buy_market(self, symbol: str, quantity: int) -> Order:
        order_function = partial(
            rh_client.order_buy_market, symbol=symbol, quantity=quantity
        )

        return self._place_order(order_function)

    def buy_limit(self, symbol: str, quantity: int, limit_price: float) -> Order:
        order_function = partial(
            rh_client.order_buy_limit,
            symbol=symbol,
            quantity=quantity,
            limitPrice=limit_price,
        )

        return self._place_order(order_function)

    def sell_market(self, symbol: str, quantity: int) -> Order:
        order_function = partial(
            rh_client.order_sell_market, symbol=symbol, quantity=quantity
        )

        return self._place_order(order_function)

    def sell_limit(self, symbol: str, quantity: int, limit_price: float) -> Order:
        order_function = partial(
            rh_client.order_sell_limit,
            symbol=symbol,
            quantity=quantity,
            limitPrice=limit_price,
        )

        return self._place_order(order_function)

    def get_order_info(self, order_id: str) -> Order:
        ts_event = time.time_ns()
        try:
            order_state = rh_client.get_stock_order_info(order_id)
        except Exception as e:
            self.logger.error(
                f"{self.account_name} - Unexpected {type(e)} when getting order {order_id}"
            )
            self.logger.error(f"{self.account_name} - {e}")

        self.orders[order_id].filled = order_state["state"] == "filled"
        if self.orders[order_id].filled:
            self.orders[order_id].state = "filled"
        self.orders[order_id].cumulative_quantity = float(
            order_state["cumulative_quantity"]
        )
        if order_state["average_price"]:
            self.orders[order_id].avg_price = order_state["average_price"]

        self.orders[order_id].events.append(
            {"type": "get_order_info", "ts": ts_event, "order_state": order_state}
        )

        return self.orders[order_id]

    def cancel_order(self, order_id: str) -> Order:
        # Get the current state of the order
        order = self.get_order_info(order_id)

        if order.filled:
            # Order filled, no need to cancel it
            return order
        else:
            # Order isn't filled, need to cancel it
            ts_cancel = time.time_ns()
            resp = rh_client.cancel_stock_order(order_id)

            if len(resp) == 0:
                self.orders[order_id].state = "cancelled"
            else:
                self.logger.error(
                    f"TDA - Failed to cancel order {order_id}. Received {resp}"
                )

            self.orders[order_id].events.append(
                {"type": "cancel_order", "ts": ts_cancel, "resp": resp}
            )

            return self.orders[order_id]

    def cleanup(self) -> None:
        return

    def _place_order(self, order_function: Callable):
        """
        Executes the order function and converts the response into an Order object
        """
        ts_order_submit = time.time_ns()
        try:
            resp = order_function()
        except Exception as e:
            self.logger.error(
                f"{self.account_name} - Unexpected {type(e)} when placing order {order_function.keywords}"
            )
            self.logger.error(f"{self.account_name} - {e}")

        try:
            order_id = resp["id"]
            self.orders[order_id] = Order(
                order_id,
                ts_order_submit,
                order_function.keywords["symbol"],
                resp["side"],
                "live",
                False,
                -1,
                order_function.keywords["quantity"],
                0,
                [],
                [order_function.keywords, resp],
            )

            return self.orders[order_id]
        except Exception as e:
            self.logger.error(
                f"{self.account_name} - Unexpected {type(e)} when parsing order"
            )
            self.logger.error(f"{self.account_name} - {e}")

    def _setup_client(self):
        """
        Authenticates the robin_stocks library with HOOD. The library only
        supports logging into a single account at a time
        """
        # Get login credentials
        try:
            secret = get_secret(self.id_secret)

            username = secret[f"u_{self.account_name}"]
            password = secret[f"p_{self.account_name}"]
            otp_seed = secret[f"totp_{self.account_name}"]
            self.logger.info(f"{self.account_name} - Successfully retrieved secrets")
        except Exception as e:
            self.logger.error(
                f"{self.account_name} - Unexpected {type(e)} when getting secrets"
            )
            self.logger.error(f"{self.account_name} - {e}")

        # Generate time-based one time password
        try:
            totp = pyotp.TOTP(otp_seed).now()
            self.logger.info(f"{self.account_name} - Successfully got TOTP")
        except Exception as e:
            self.logger.error(
                f"{self.account_name} - Unexpected {type(e)} when getting TOTP"
            )
            self.logger.error(f"{self.account_name} - {e}")

        # Log into Robinhood
        try:
            login = rh_client.login(
                username, password, mfa_code=totp, store_session=False, cred_path="/tmp"
            )
            self.logger.info(f"{self.account_name} - Successfully logged in")
        except Exception as e:
            self.logger.error(
                f"{self.account_name} - Unexpected {type(e)} when logging into account"
            )
            self.logger.error(f"{self.account_name} - {e}")


class PaperAccountPolygon(Broker):
    """
    Paper trading account implemented using data from Polygon.io

    Args:
        date (`str`, *optional*):
            Specific date, in ISO 8601 format (YYYY-MM-DD), to use when getting historical pricing data, e.g., for back-testing
    """

    uri_quotes = "https://api.polygon.io/v3/quotes/{symbol}?timestamp.lte={timestamp}&limit={n}&order=desc"
    uri_trades = "https://api.polygon.io/v3/trades/{symbol}?timestamp.lte={timestamp}&limit={n}&order=desc"

    def __init__(self, logger, date=None) -> None:
        self.logger = logger
        self.orders = []
        self.api_key = os.environ.get("PG_API_KEY")
        self.client = Session()

        if date:
            # Running as of a specific date
            self.t_init = time.time_ns()
            try:
                # Determine market open on that date in seconds
                market_open = time.mktime(
                    datetime.strptime(date, "%Y-%m-%d").timetuple()
                ) + ((9.5 - 3) * 60 * 60)

                # Set the offset in nanoseconds
                self.offset = int(market_open * 1e9)
            except:
                raise ValueError("date must be in the format YYYY-MM-DD")
        else:
            # Running in real-time
            self.t_init = 0
            self.offset = 0

    def buy_market(self, symbol: str, quantity: int) -> Order:
        return self._market_order(symbol, quantity, "buy")

    def buy_limit(self, symbol: str, quantity: int, limit_price: float) -> Order:
        return self._limit_order(symbol, quantity, limit_price, "buy")

    def sell_market(self, symbol: str, quantity: int) -> Order:
        return self._market_order(symbol, quantity, "sell")

    def sell_limit(self, symbol: str, quantity: int, limit_price: float) -> Order:
        return self._limit_order(symbol, quantity, limit_price, "sell")

    def get_order_info(self, order_id: int) -> Order:
        order = self.orders[order_id]

        if order.filled or order.state == "cancelled":
            # Order state is fixed
            return order
        else:
            # Order is live and unfilled, update the state of the order
            # Current time may be relative to a back-test time
            ts = time.time_ns() - self.t_init + self.offset

            # Get market data
            uri = self.uri_trades.format(symbol=order.symbol, timestamp=ts, n=1000)
            resp = self.client.get(uri, params={"apiKey": self.api_key})
            assert resp.status_code == 200, "Request for market data failed"
            trades = resp.json()["results"]

            for trade in reversed(trades):  # Query returns trades desc by ts
                if trade["sip_timestamp"] > order.t_created:
                    price = trade["price"]
                    if order.side == "buy":
                        filled = order.avg_price >= price
                    else:
                        filled = order.avg_price <= price

                    if filled:
                        # Update order status to reflect fill
                        order.state = "filled"
                        order.filled = filled
                        order.cumulative_quantity = order.quantity
                        break

            return order

    def cancel_order(self, order_id: int) -> Order:
        order = self.orders[order_id]
        if order.state == "live":
            order = self.get_order_info(order_id)

        if not order.filled:
            order.state = "cancelled"

        return order

    def _market_order(self, symbol: str, quantity: int, direction: str) -> Order:
        order_id = len(self.orders)

        # Current time may be relative to a back-test time
        ts = time.time_ns() - self.t_init + self.offset

        # Get market info from this time
        uri = self.uri_quotes.format(symbol=symbol, timestamp=ts, n=1)
        resp = self.client.get(uri, params={"apiKey": self.api_key})
        assert resp.status_code == 200, "Request for market data failed"

        if direction == "buy":
            # Assume you pay the NBO
            avg_price = resp.json()["results"][0]["ask_price"]
        elif direction == "sell":
            # Assume you pay the NBB
            avg_price = resp.json()["results"][0]["bid_price"]
        else:
            raise ValueError("direction must be buy or sell")

        self.orders.append(
            Order(
                order_id,
                ts,
                symbol,
                direction,
                "filled",
                True,
                avg_price,
                quantity,
                quantity,
                [],
                [],
            )
        )

        return self.orders[-1]

    def _limit_order(
        self, symbol: str, quantity: int, limit_price: float, direction: str
    ) -> Order:
        order_id = len(self.orders)

        # Current time may be relative to a back-test time
        ts = time.time_ns() - self.t_init + self.offset

        # Get market info from this time
        uri = self.uri_quotes.format(symbol=symbol, timestamp=ts, n=1)
        resp = self.client.get(uri, params={"apiKey": self.api_key})
        assert resp.status_code == 200, "Request for market data failed"

        state = "live"
        filled = False
        if direction == "buy":
            # Assume you pay the NBO
            ask_price = resp.json()["results"][0]["ask_price"]
            if ask_price <= limit_price:
                avg_price = ask_price
                state = "filled"
                filled = True
            else:
                avg_price = limit_price
        elif direction == "sell":
            # Assume you pay the NBB
            bid_price = resp.json()["results"][0]["bid_price"]
            if bid_price >= limit_price:
                avg_price = bid_price
                state = "filled"
                filled = True
            else:
                avg_price = limit_price
        else:
            raise ValueError("direction must be buy or sell")

        cumulative_quantity = 0
        if filled:
            cumulative_quantity = quantity

        self.orders.append(
            Order(
                order_id,
                ts,
                symbol,
                direction,
                state,
                filled,
                avg_price,
                quantity,
                cumulative_quantity,
                [],
                [],
            )
        )

        return self.orders[-1]

    def _paginate(self, url, max_pages=None) -> list:
        results = []
        next_url = url
        n = 0

        while next_url:
            try:
                resp = self.client.get(next_url, params={"apiKey": self.api_key})
            except Exception as e:
                print(f"Unexpected {type(e)}: {url}")
                return []
            n += 1

            if resp.status_code == 200:
                data = resp.json()
                if "results" in data:
                    results.extend(data["results"])

                if "next_url" in data:
                    next_url = data["next_url"]
                else:
                    next_url = None

            else:
                print(f"{resp.status_code} error when getting page: {next_url}")
                return []

            if max_pages is not None:
                if n >= max_pages:
                    print("Hit pagination limit")
                    break

        return results
