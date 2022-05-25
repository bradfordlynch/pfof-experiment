import argparse
import base64
from collections.abc import Callable
import secrets
from datetime import datetime
from functools import partial
import json
import logging
import logging.handlers
import multiprocessing
from multiprocessing import Queue
import os
import pickle
from queue import Empty
import requests
import time

import boto3
from trading import Broker, PaperAccountPolygon, IBAccount, TDAAccount, RobinhoodAccount
from utils import generate_experiment, RobustEncoder, TelegramHook

PG_API_KEY = os.environ.get("PG_API_KEY")
MAX_WAIT_BEFORE_CANCEL_MIN = 5  # Minutes
ACCOUNT_NAME_BROKER_MAP = {
    "IBKR": IBAccount,
    "TDA": TDAAccount,
    "Robinhood": RobinhoodAccount,
}


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--exp",
        type=str,
        default="noise_trade_experiment.json",
        help="path to experiment file to run",
    )
    parser.add_argument(
        "--run_as_test",
        type=bool,
        default=False,
        help="whether to run the experiment using paper accounts",
    )
    parser.add_argument(
        "--gen_experiment",
        type=bool,
        default=False,
        help="generates a set of trades for testing",
    )
    parser.add_argument(
        "--aws_secret",
        type=str,
        default="pfof-exp",
        help="name or ARN of secrets in AWS",
    )
    parser.add_argument(
        "--aws_bucket",
        type=str,
        default="pfof-experiment",
        help="AWS bucket for storing experiment results",
    )
    parser.add_argument(
        "--telegram_chat_id",
        type=str,
        default=None,
        help="Telegram chat Id for sending status updates",
    )
    args = parser.parse_args()

    return args


def _listener_configurer(fn_log: str) -> None:
    root = logging.getLogger()
    h = logging.FileHandler(fn_log)
    f = logging.Formatter("%(asctime)s %(processName)-10s %(levelname)-8s %(message)s")
    h.setFormatter(f)
    root.addHandler(h)


def worker_configurer(queue: Queue) -> None:
    h = logging.handlers.QueueHandler(queue)
    root = logging.getLogger()
    if not root.handlers:
        root.addHandler(h)
    root.setLevel(logging.INFO)


def _listener_process(
    queue: Queue,
    configurer: Callable,
    fn_log: str,
    id_secret: str,
    id_telegram_chat: str,
) -> None:
    """Queue-based logger for logging to the same file from many processes

    https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
    """
    configurer(fn_log)
    logger = logging.getLogger()

    if id_telegram_chat:
        telegram = TelegramHook(logger, id_secret, id_telegram_chat)

    while True:
        try:
            record = queue.get()
            if record is None:
                break
            logger.handle(record)
        except Exception:
            import sys, traceback

            print("Exception in logger", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)

        if id_telegram_chat:
            try:
                is_main = record.processName == "MainProcess"
                is_error = record.levelname == "ERROR"
                is_placed_order = "Placed order: " in record.message
                is_pos_status = "Position status at cancel " in record.message
                is_closed = "Closed position: " in record.message
                is_ibkr_warn = "Order Event Warning" in record.message
                if (
                    is_main or is_error or is_placed_order or is_pos_status or is_closed
                ) and not is_ibkr_warn:
                    telegram.send_message(record.message)
            except Exception as e:
                logger.error(f"Unexpected {type(e)} when sending Telegram message")


def _setup_mp_logger(fn_log: str, id_secret: str, id_telegram_chat: str):
    """Sets up multiprocessing based logger"""
    queue = Queue(-1)
    listener = multiprocessing.Process(
        target=_listener_process,
        args=(queue, _listener_configurer, fn_log, id_secret, id_telegram_chat),
    )
    listener.start()

    h = logging.handlers.QueueHandler(queue)
    logger = logging.getLogger()
    if not logger.handlers:
        logger.addHandler(h)
    logger.setLevel(logging.INFO)

    return queue, logger, listener


def _trading_process(
    action_queue: Queue,
    logging_queue: Queue,
    observation_queues: list,
    configurer: Callable,
    account_constructor: Broker,
) -> None:
    """Function to handle interactions with brokers."""
    # Setup within process logging
    configurer(logging_queue)
    logger = logging.getLogger()

    # Setup brokerage account
    account = account_constructor(logger)

    while True:
        # Accounts try to handle errors gracefully but wrap the trading loop
        # in a try/except block to catch anything unexpected
        try:
            msg = action_queue.get()
            t_recv = time.time_ns()

            if msg is None:
                # Stop signal
                account.cleanup()
                break

            if msg["action"] == "buy":
                if msg["order_type"] == "market":
                    order = account.buy_market(msg["symbol"], msg["quantity"])
                else:
                    order = account.buy_limit(
                        msg["symbol"], msg["quantity"], msg["limit_price"]
                    )
            elif msg["action"] == "sell":
                if msg["order_type"] == "market":
                    order = account.sell_market(msg["symbol"], msg["quantity"])
                else:
                    order = account.sell_limit(
                        msg["symbol"], msg["quantity"], msg["limit_price"]
                    )
            elif msg["action"] == "cancel":
                order = account.cancel_order(msg["order_id"])
            elif msg["action"] == "get_order":
                order = account.get_order_info(msg["order_id"])
            else:
                raise NotImplementedError(f"Unsupported action: {msg['action']}")

            t_resp = time.time_ns()

            msg.update({"order": order, "t_acct_recv": t_recv, "t_acct_resp": t_resp})

            observation_queues[msg["ob_id"]].put_nowait(msg)
        except Exception as e:
            logger.critical(
                f"{account.account_name} - Unexpected {type(e)} when trading {msg}"
            )


def _observation_process(
    obs_queue: Queue,
    account_queues: dict,
    observation: dict,
    logging_queue: Queue,
    configurer: Callable,
    final_results_queue: Queue,
) -> None:
    # Setup within process logging
    configurer(logging_queue)
    logger = logging.getLogger()
    logger.info(f'Ob {observation["id"]} - {observation}')

    # Basic observation features and action
    account_name = observation["account"]
    symbol = observation["symbol"]
    ts_open_utc_ns = observation["ts_open_utc_ns"]
    ts_close_utc_ns = observation["ts_close_utc_ns"]
    buy = {
        "ob_id": observation["id"],
        "action": "buy",
        "symbol": observation["symbol"],
        "order_type": observation["order_type"],
    }
    observation["events"] = []

    # Determine order timing
    # Process sleeps until 15 seconds before order submission
    t_sleep = max((ts_open_utc_ns - time.time_ns()) / 1e9 - 15, 0)
    logger.info(
        f'Ob {observation["id"]} - Sleeping {t_sleep:.0f} seconds before submitting order'
    )
    time.sleep(t_sleep)

    # We need to get the NBBO to determine order size in shares and
    # limit prices for non-market orders. This could be done using a
    # websocket client to stream data directly from IBKR, TDA, or
    # HOOD, but testing showed that all of these were slower than
    # Polygon.io's feed which is located in the datacenters with NYSE,
    # NASDAQ, BATS, IEX, etc. Further testing showed that Polygon.io's
    # REST endpoint for the last NBBO provided the most up-to-date
    # state of the market, with around 21ms latency. The websocket API
    # was around 150ms (from us-east-1-nyc-1a).
    while True:
        # Average latency from Polygon is 21ms
        if time.time_ns() >= (ts_open_utc_ns - 22 * 1e6):
            # Get the most recent NBBO
            uri = f"https://api.polygon.io/v2/last/nbbo/{symbol}"
            observation["t_nbbo_req"] = time.time_ns()
            resp = requests.get(uri, params={"apiKey": PG_API_KEY})
            observation["t_nbbo_resp"] = time.time_ns()

            logger.info(f"{resp} - {resp.json()}")

            observation["nbbo"] = resp.json()["results"]

            try:
                nbb, nbo = observation["nbbo"]["p"], observation["nbbo"]["P"]
            except Exception as e:
                logger.error(f"Unexpected {type(e)} when getting NBBO data")
                logger.error(str(e))
                logger.error(resp.json())

            if observation["order_type"] != "limit":
                # For market and marketable limit orders, the order
                # size and price are based on the NBO
                limit_price = nbo
            else:
                # For limit orders, the order size and price are
                # based on a random price within the half spread
                mid = (nbb + nbo) / 2
                half_spread = nbo - mid
                limit_price = round(mid + half_spread * observation["limit_price"], 2)

            buy["limit_price"] = limit_price

            if limit_price <= 0:
                logger.error(
                    f'Ob {observation["id"]} - Invalid price of ${limit_price:.02f} for {symbol}, aborting trade'
                )
                logger.info(observation)
                return False

            # Convert to shares, buying at least one share
            buy["quantity"] = max(round(observation["order_size"] / limit_price), 1)
            break

    # Nearing time to submit order, loop until time to submit
    while True:
        ts_now = time.time_ns()
        if ts_now >= ts_open_utc_ns:
            account_queues[account_name].put_nowait(buy)
            observation["ts_open_req"] = ts_now
            break

    # Wait for response from trading account
    msg = obs_queue.get()
    order = msg["order"]

    # If order is None then there was some catastrophic error
    if order is None:
        logger.critical(
            f'Ob {observation["id"]} - Error opening position for {symbol}: {msg}'
        )
        return None

    # Successfully placed order to open position
    observation["ts_open_resp"] = time.time_ns()
    logger.info(f'Ob {observation["id"]} - Placed order: {msg}')
    observation["events"].append(msg)

    # If the order didn't fill, then we wait until it does fill or
    # up to five minutes before cancelling the order
    ts_cancel = ts_open_utc_ns + MAX_WAIT_BEFORE_CANCEL_MIN * 60 * 1e9
    while not order.filled:
        ts_now = time.time_ns()
        if ts_now >= ts_cancel:
            account_queues[account_name].put_nowait(
                {
                    "ob_id": observation["id"],
                    "action": "cancel",
                    "order_id": order.order_id,
                }
            )
            observation["ts_cancel_req"] = ts_now

            # Wait for response from trading account
            msg = obs_queue.get()
            order = msg["order"]
            observation["ts_cancel_resp"] = time.time_ns()
            observation["events"].append(msg)
            logger.info(
                f'Ob {observation["id"]} - Position status at cancellation: {msg}'
            )
            break
        else:
            # Sleep until it is time to cancel the order
            t_sleep = max(0.0001, (ts_cancel - ts_now) / 1e9)
            logger.info(
                f'Ob {observation["id"]} - Sleeping {t_sleep:.0f} seconds before cancelling order'
            )
            time.sleep(t_sleep)

    observation["order_to_open"] = order

    # Log intermeadiate results
    try:
        logger.info(
            f'Ob {observation["id"]} - POST_BUY_JSON - {json.dumps(observation, cls=RobustEncoder)}'
        )
    except Exception as e:
        logger.error(
            f'Ob {observation["id"]} - Unexpected {type(e)} when serializing buy result to JSON'
        )
        logger.error(f'Ob {observation["id"]} - POST_BUY_STR_JSON - {observation}')

    try:
        logger.info(
            f'Ob {observation["id"]} - POST_BUY_PKL - {base64.b64encode(pickle.dumps(observation)).decode()}'
        )
    except Exception as e:
        logger.error(
            f'Ob {observation["id"]} - Unexpected {type(e)} when pickling buy result'
        )
        logger.error(f'Ob {observation["id"]} - POST_BUY_STR_PKL - {observation}')

    # Prepare to sell position
    if order.cumulative_quantity > 0:
        sell = {
            "ob_id": observation["id"],
            "action": "sell",
            "symbol": symbol,
            "order_type": "market",
            "quantity": order.cumulative_quantity,
        }

        while True:
            ts_now = time.time_ns()
            if ts_now >= ts_close_utc_ns:
                # Shut'er down now
                account_queues[account_name].put_nowait(sell)
                observation["ts_close_req"] = ts_now
                break
            elif (ts_close_utc_ns - ts_now) / 1e9 >= 2:
                # Sleep until one sec before it is time to close the position
                t_sleep = max(0.0001, (ts_close_utc_ns - ts_now) / 1e9 - 1)
                logger.info(
                    f'Ob {observation["id"]} - Sleeping {t_sleep:.0f} seconds before closing position'
                )
                time.sleep(t_sleep)

        # Get order result
        msg = obs_queue.get()
        observation["events"].append(msg)

        # Get final state of order
        time.sleep(5)  # Wait a few seconds before checking the state
        sell_order = msg["order"]
        try:
            account_queues[account_name].put_nowait(
                {
                    "ob_id": observation["id"],
                    "action": "get_order",
                    "order_id": sell_order.order_id,
                }
            )
            msg = obs_queue.get()
            sell_order = msg["order"]
            observation["order_to_close"] = sell_order
            logger.info(f'Ob {observation["id"]} - Closed position: {msg}')
            observation["events"].append(msg)
        except Exception as e:
            logger.error(
                f'Ob {observation["id"]} - Unexpected {type(e)} when closing position'
            )
    else:
        logger.info(f'Ob {observation["id"]} - Position never filled, no need to close')

    try:
        logger.info(
            f'Ob {observation["id"]} - FINAL_JSON - {json.dumps(observation, cls=RobustEncoder)}'
        )
    except Exception as e:
        logger.error(
            f'Ob {observation["id"]} - Unexpected {type(e)} when serializing final result to JSON'
        )
        logger.error(f'Ob {observation["id"]} - FINAL_STR_JSON - {observation}')

    try:
        logger.info(
            f'Ob {observation["id"]} - FINAL_PKL - {base64.b64encode(pickle.dumps(observation)).decode()}'
        )
    except Exception as e:
        logger.error(
            f'Ob {observation["id"]} - Unexpected {type(e)} when pickling final result'
        )
        logger.error(f'Ob {observation["id"]} - FINAL_STR_PKL - {observation}')

    final_results_queue.put_nowait(observation)

    return None


if __name__ == "__main__":
    today = datetime.now().strftime("%Y-%m-%d")
    args = _parse_args()

    # Setup logging
    fn_log = f'{args.exp.rsplit(".", 1)[0]}_{today}_{secrets.token_urlsafe(8)}.log'
    logging_queue, logger, listener = _setup_mp_logger(
        fn_log, args.aws_secret, args.telegram_chat_id
    )

    # Load experiment design
    with open(args.exp, "r") as in_file:
        experiment = json.load(in_file)

    if args.gen_experiment:
        obs = generate_experiment()
        logger.info(f"Test experiment has {len(obs)} observations to run")
    else:
        obs = [ob for ob in experiment if ob["date_open"] == today]
        logger.info(f"Today has {len(obs)} observations to run")

    # Setup queues for observations and brokers
    obs_queues = {ob["id"]: Queue(-1) for ob in obs}
    account_names = set([ob["account"] for ob in obs])
    account_queues = {k: Queue(-1) for k in account_names}
    final_results_queue = Queue(-1)

    # Setup processes for trading
    trading_proc_inputs = []
    if args.run_as_test:
        logger.info("Running experiment using paper accounts")
        for name, q in account_queues.items():
            trading_proc_inputs.append(
                (
                    q,
                    logging_queue,
                    obs_queues,
                    worker_configurer,
                    PaperAccountPolygon,
                )
            )
    else:
        logger.warning("Running experiment using real-ish money")
        n_ibkr = 0
        n_hood = 0
        for name, q in account_queues.items():
            account_type = name.split("_")[0]
            if account_type == "IBKR":
                try:
                    # The IBAccount connects to TWS which is a single account.
                    # As a result, only one IB account is supported at a time.
                    # Could run multiple instances of TWS with different ports
                    # if multiple accounts is necessary...
                    assert n_ibkr == 0
                    n_ibkr += 1
                except AssertionError:
                    raise NotImplementedError("Multiple IB accounts is not supported")
                constructor = IBAccount
            elif account_type == "TDA":
                constructor = partial(
                    TDAAccount,
                    id_secret=args.aws_secret,  # Credentials are stored in AWS
                    account_name=name,  # Account name associated with credentials
                )
            elif account_type == "Robinhood":
                try:
                    # assert n_hood == 0
                    n_hood += 1
                except AssertionError:
                    raise NotImplementedError(
                        "Multiple Robinhood accounts is not supported"
                    )
                constructor = partial(
                    RobinhoodAccount,
                    id_secret=args.aws_secret,  # Credentials are stored in AWS
                    account_name=name,  # Account name associated with credentials
                )
            else:
                raise NotImplementedError(f"Unsupported account {name}")

            trading_proc_inputs.append(
                (
                    q,
                    logging_queue,
                    obs_queues,
                    worker_configurer,
                    constructor,
                )
            )

    trading_procs = []
    for inputs in trading_proc_inputs:
        worker = multiprocessing.Process(target=_trading_process, args=inputs)
        trading_procs.append(worker)
        worker.start()

    # Setup processes for each observation
    obs_procs = []
    for ob in obs:
        worker = multiprocessing.Process(
            target=_observation_process,
            args=(
                obs_queues[ob["id"]],
                account_queues,
                ob,
                logging_queue,
                worker_configurer,
                final_results_queue,
            ),
        )
        obs_procs.append(worker)
        worker.start()

    ts_max = max([ob["ts_close_utc_ns"] for ob in obs]) / 1e9
    t_sleep = ts_max - time.time() + 60  # MUST BE IN SECONDS FOR time.sleep
    logger.info(f"Sleeping for {t_sleep:.0f} while experiment runs")
    time.sleep(t_sleep)

    # Let the experiment run
    logger.info("Joining observation processes")
    for worker in obs_procs:
        worker.join(1)
        logger.info(f"Joined {worker}")
    logger.info("All observation processes exited")

    # Shutdown trading processes
    for k, q in account_queues.items():
        q.put_nowait(None)

    for worker in trading_procs:
        worker.join(10)
    logger.info("All account processes exited")

    # Put experiment results in S3
    logger.info("Putting results in S3")
    s3 = boto3.client("s3")
    exp_folder = os.path.splitext(fn_log)[0]

    results = []
    while True:
        try:
            results.append(final_results_queue.get_nowait())
        except Empty:
            break
        except Exception as e:
            logger.error(f"Unexpected {type(e)} when collecting final results")

    s3.put_object(
        Bucket=args.aws_bucket,
        Key=os.path.join(exp_folder, "result_objects.pkl"),
        Body=pickle.dumps(results),
    )
    logger.info("Uploaded result objects to S3")

    logger.info("Uploading logs to S3, have a nice day")
    s3.upload_file(
        Filename=fn_log, Bucket=args.aws_bucket, Key=os.path.join(exp_folder, fn_log)
    )
    print("Uploaded logs")

    # Shut down logging
    print("Shutting down logging")
    logging_queue.put_nowait(None)
    listener.join(10)
