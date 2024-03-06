# Automated Trading Experiments
Infrastructure to place trades according to a schedule or "experiment." The code was developed with the specific goal of running randomized experiments across a variety of brokers to measure order execution quality at payment for order flow (PFOF) and non-PFOF brokers. However it is fairly generic and can be used for automated trading more generally.

**If you use this code, please cite the paper for which it was developed:**

```
Levy (Lynch), Bradford, Price Improvement and Payment for Order Flow: Evidence from A Randomized Controlled Trial (June 27, 2022). Jacobs Levy Equity Management Center for Quantitative Financial Research Paper , Available at SSRN: https://ssrn.com/abstract=4189658 or http://dx.doi.org/10.2139/ssrn.4189658
```

## Architecture
### Key Services
Currently, the code supports IBKR, TDA, and Robinhood. The physical location of these services varies, e.g., IBKR in NY5 and Robinhood in us-east-1. To minimize and/or document the latency to these services, one may wish to monitor the latency and optimize where the trading code runs. The key service addresses are:
- IBKR: ndc1.ibllc.com
- Robinhood: api.robinhood.com
- TDA: api.tdameritrade.com

## Environment setup
The experiment code expects the following environment variables be set.
```
export PG_API_KEY="your Polygon.io credentials"
export AWS_DEFAULT_REGION="your-aws-region"
```

## GUI
Trading on IBKR requires that a "gateway" be running on the host. Unfortunately, the gateways provided by IBKR are not designed for headless operation. Thus, a GUI is needed. Since the experiment is run from a datacent, DCV is used to serve GUI access for interacting with IBKR. After starting the instance, we need to create a DCV session:

```
dcv create-session {session_name}
```

We can then connect to the instance and start an IBKR gateway (either the Client Gateway or Trader Workstation).

## Running an experiment
Actually running an experiment requires passing a JSON file specifying the trades that should be executed:
```
python main.py --exp pfof_experiment_20220525.json
```
The code will then run until markets close. See the file `pfof_experiment_20220525.json` for an example of the expected input format.