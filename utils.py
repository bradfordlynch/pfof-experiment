from datetime import datetime
import json
import time

import boto3


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
            for order_type in ["limit"]:  # , "market", "marketable_limit"]:
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
                    ob.update({"order_type": order_type, "limit_price": -200})
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
