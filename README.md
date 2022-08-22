# Automated Trading Experiments
Infrastructure to place trades according to a schedule or "experiment." The code was developed with the specific goal of running randomized experiments across a variety of brokers to measure order execution quality at payment for order flow (PFOF) and non-PFOF brokers. However it is fairly generic and can be used for automated trading in general.

## Architecture
### Key Services
Currently, the code supports IBKR, TDA, and Robinhood. The physical location of these services varies, e.g., IBKR in NY5 and Robinhood in us-east-1. To minimize and/or document the latency to these services, one may wish to monitor the latency and optimize where the trading code runs. The key service addresses are:
- IBKR: ndc1.ibllc.com
- Robinhood: api.robinhood.com
- TDA: api.tdameritrade.com

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
python main.py --exp pfof_experiment_20220525.json --telegram_chat_id 1387574277
```
