import time
import multiprocessing
from multiprocessing import Queue

from market_data import PolygonWebsocketProvider
from utils import PrintLogger


def market_data_process(logger, directives_queue, receivers):
    data_provider = PolygonWebsocketProvider(
        logger, directives_queue, receivers, "wss://socket.polygon.io/stocks"
    )
    logger.info("Starting market data")
    # Blocks
    data_provider.run()

    data_provider.close()


def consumer_process(logger, observation, directives_queue, market_data_queue):
    id_ob = observation["id"]
    sub_ob = observation["sub"]
    logger.info(f"Ob {id_ob} - Sleeping for {id_ob} sec")
    time.sleep(id_ob)

    directives_queue.put([{"action": "subscribe", "stream": sub_ob, "id": id_ob}])

    while True:
        msg = market_data_queue.get()
        try:
            ts_cons_recv = time.time()
            lag_pg = ts_cons_recv * 1000 - msg["t"]
            lag_int = (ts_cons_recv - msg["ts_recv"]) * 1000
            logger.info(f"Ob {id_ob} - {lag_pg:.1f} ms, {lag_int:.3f} ms")
        except:
            logger.error(f"Ob {id_ob} - {msg}")


if __name__ == "__main__":
    tickers = ["IWM", "QQQ"]
    obs = [{"id": i, "sub": f"Q.{ticker}"} for i, ticker in enumerate(tickers)]
    logger = PrintLogger()
    q_directives = Queue(-1)
    receivers = {ob["id"]: Queue(-1) for ob in obs}
    logger.info("Starting market data process")
    mdp = multiprocessing.Process(
        target=market_data_process, args=(logger, q_directives, receivers), daemon=True
    )
    mdp.start()

    logger.info("Starting consumer processes")
    consumers = [
        multiprocessing.Process(
            target=consumer_process,
            args=(logger, ob, q_directives, receivers[ob["id"]]),
            daemon=True,
        )
        for ob in obs
    ]
    for consumer in consumers:
        consumer.start()

    time.sleep(10)

    logger.info("Joining processes")
    mdp.join(5)

    for consumer in consumers:
        consumer.join(1)

    logger.info("Exiting")
