import argparse
from math import sqrt
import os
import random
import time

import requests

pg_api_key = os.environ.get("PG_API_KEY")


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--symbols",
        type=str,
        default="AAPL,MSFT,NFLX,SPY,QQQ,IWM",
        help="comma seperated list of symbols or path to file containing symbols",
    )
    parser.add_argument(
        "--n_samples",
        type=int,
        default=100,
        help="how many requests to make",
    )

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = _parse_args()

    symbols = [symbol.strip().upper() for symbol in args.symbols.split(",")]

    dts = []

    for i in range(args.n_samples):
        symbol = random.choice(symbols)
        uri = f"https://api.polygon.io/v2/last/nbbo/{symbol}"
        _start = time.time()
        resp = requests.get(uri, params={"apiKey": pg_api_key})
        dts.append((time.time() - _start) * 1000)

        try:
            assert resp.status_code == 200, "Non-200 response from API"
        except Exception as e:
            print(uri)
            print(resp)
            print(resp.content)

    mu = sum(dts) / len(dts)
    std = sqrt(sum([(dt - mu) ** 2 for dt in dts]) / len(dts))

    print(f"Result of {args.n_samples} runs: {mu:.1f} +- {std:.3f} ms")
