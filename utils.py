from datetime import datetime
import json
import time

import boto3
import requests


def get_secret(id_secret):
    """
    Gets a secret from AWS
    """
    client_secrets = boto3.client("secretsmanager")
    resp = client_secrets.get_secret_value(SecretId=id_secret)

    assert (
        resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    ), "Secrets get failed with non-200 status"

    return json.loads(resp["SecretString"])


def put_secret(id_secret, secret):
    """
    Puts a secret in AWS
    """
    client_secrets = boto3.client("secretsmanager")
    resp = client_secrets.put_secret_value(
        SecretId=id_secret, SecretString=json.dumps(secret)
    )

    assert (
        resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    ), "Secrets put failed with non-200 status"

    return True


def generate_experiment(
    accounts=["Robinhood_Alpha", "TDA", "IBKR"], lag=10, n_orders=1
):
    today = datetime.now().strftime("%Y-%m-%d")
    i_obs = 0
    obs = []
    # Create trades starting lag seconds from now
    ts_start = int(time.time_ns() + lag * 1e9)
    ts_open = ts_start

    for i in range(n_orders):
        for account in accounts:
            for order_type in ["limit", "market"]:  # , "market", "marketable_limit"]:
                ob = {
                    "id": i_obs,
                    "date_open": today,
                    "account": account,
                    "symbol": "SPY",
                    "ts_open_utc_ns": ts_open,
                    "ts_close_utc_ns": int(ts_open + 5.5 * 60 * 1e9),
                    "order_size": 1,  # Dollars
                }
                if order_type == "limit":
                    ob.update({"order_type": order_type, "limit_price": -50})
                elif order_type == "market":
                    ob.update(
                        {
                            "order_type": order_type,
                        }
                    )
                elif order_type == "marketable_limit":
                    ob.update(
                        {
                            "order_type": order_type,
                        }
                    )
                else:
                    raise NotImplementedError(f"Unsupported order type {order_type}")

                obs.append(ob)

                # Each trade will be seperated by 60 seconds
                ts_open += int(10 * 1e9)
                i_obs += 1

    return obs


class RobustEncoder(json.JSONEncoder):
    """
    JSONEncoder that is robust to objects lacking a `to_json` method. Attempts
    to serialize such objects using the __dict__ attribute and falls back to
    a `str` representation when that fails.
    """

    def default(self, obj):
        try:
            return json.JSONEncoder.default(self, obj)
        except TypeError:
            try:
                return obj.__dict__
            except Exception as e:
                print(f"Unexpected {type(e)} when encoding {obj}")
                return str(obj)


class TelegramHook:
    """
    Minimal integration with Telegram to send status updates
    """

    def __init__(self, logger, id_secret: str, id_chat: str) -> None:
        self.logger = logger
        secrets = get_secret(id_secret)
        self.bot_token = secrets["telegram_hook"]
        self.id_chat = id_chat

    def send_message(self, msg: str) -> bool:
        uri = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        params = {"chat_id": self.id_chat, "parse_mode": "Markdown", "text": msg}

        try:
            resp = requests.get(uri, params=params)
            assert resp.status_code == 200
            return True
        except AssertionError:
            self.logger.error("Received non-200 status code from Telegram API")
        except Exception as e:
            self.logger.error(f"Unexpected {type(e)} when calling Telegram API")

        return False


class PrintLogger:
    """
    Hack to print shit
    """

    def __init__(self) -> None:
        pass

    def debug(self, output):
        self._print("DEBUG", output)

    def info(self, output):
        self._print("INFO", output)

    def warn(self, output):
        self._print("WARN", output)

    def error(self, output):
        self._print("ERROR", output)

    def _print(self, level, output):
        print(f"{level} - {output}")
