# Automated Trading Experiments
Infrastructure to place trades according to a schedule or "experiment." The code was developed with the specific goal of running randomized experiments across a variety of brokers to measure order execution quality at at payment for order flow (PFOF) and non-PFOF brokers. However it is fairly generic and can be used for automated trading in general.

## Architecture


## GUI
DCV is used to serve GUI access for interacting with IBKR. After starting the instance, we need to create a DCV session:

```
dcv create-session {session_name}
```

## Environment setup
The experiment code expects the following environment variables be set.
```
export PG_API_KEY="your Polygon.io credentials"
export AWS_DEFAULT_REGION="your-aws-region"
```

## Running an experiment
```
python main.py --exp noise_trade_test.json --gen_experiment True --telegram_chat_id 1387574277
```