# Model card

## What it is

A LightGBM binary classifier that estimates, for one transaction at the moment it is submitted, how likely it
is to be fraud. It runs beside a small set of fixed rules; the stored `risk_score` and reason codes come from
both. The deployed model is `models/current` (version `v0.2-synthetic`).

## Intended use

Ranking transactions so that a fraud analyst reviews the riskiest ones first. The output is a risk estimate for
a reviewer. It is not a banking or regulatory decision and must not be the sole basis for blocking a customer.

## Inputs

18 features computed from the transaction and the account's recent history: amount and its logarithm,
transaction type, hour of day, amount against balance, whether it drains the balance, whether the destination
balance is zero, counts and sums over 60 seconds, 1 hour and 24 hours, the 24-hour average amount, seconds
since the previous transaction, whether the counterparty or device is new for the account, and how many
distinct senders paid the destination in the last hour.

The same code (`libs/fraudlib/features.py`) computes them in training and in live scoring, and a test asserts
the two agree. The model sees no name, address, age, gender or other personal attribute.

## Training data: none of it is real

| Model | Data | Where measured |
|---|---|---|
| `v0.2-synthetic` (deployed) | 200,000 events from this project's own simulator, 1.2% fraud | `reports/train_v0.2-synthetic.json` |
| `paysim-500k-v2` (not deployed, not in the repository) | First 500,000 rows of PaySim, a public simulator of mobile-money transfers: 233 frauds | Trained on the author's laptop |
| `paysim-full` (not deployed, not in the repository) | All 6,362,620 PaySim rows: 8,213 frauds | Trained on the author's laptop, 15 Nov 2026 |

Neither is real UPI, card or bank data. **No figure below says anything about performance on real fraud.**

## Results

Time-based split: first 70% train, next 15% validation (threshold choice), last 15% test, touched once. The
threshold is the score that flags 1% of validation traffic.

Simulator data, test set (deployed model):

| Model | PR-AUC | Precision | Recall |
|---|---|---|---|
| Rules only | 0.581 | 0.692 | 0.580 |
| Logistic regression | 0.985 | 0.997 | 0.922 |
| Random forest | 0.997 | 1.000 | 0.691 |
| LightGBM | 0.996 | 1.000 | 0.808 |

These are too good to mean much: the simulator's fraud patterns are simple and the model learns the simulator.

PaySim, first 500,000 rows, test set (only 21 frauds, so every figure is fragile):

| Model | PR-AUC | Notes |
|---|---|---|
| Rules only | 0.0011 | |
| Logistic regression | 0.2527 | |
| LightGBM | 0.7595 | ROC-AUC 0.9823, 20 of 21 frauds caught, 1.01% of legitimate transactions flagged |
| Random forest | 0.9006 | Better than the deployed model type on this data |

PaySim, all 6.3 million rows, test set (4,009 frauds among 954,396 transactions):

| Model | PR-AUC | Precision | Recall | False-positive rate |
|---|---|---|---|---|
| Rules only | 0.0202 | 0.020 | 0.859 | 0.1734 |
| Logistic regression | 0.2445 | 0.252 | 0.394 | 0.0049 |
| Random forest | 0.9998 | 0.349 | 1.000 | 0.0079 |
| LightGBM | 0.9815 | 0.321 | 1.000 (4,008 of 4,009) | 0.0089 (8,488 false alarms) |

Near-perfect ranking here is mostly PaySim's doing: its fraud empties the sender's balance, and the top two
features (amount against balance, drains the balance) read that directly. Real fraud is rarely that tidy. The
run shows the pipeline handles the full dataset; it is not evidence of real-world accuracy. Training took
684 seconds on a laptop CPU and peaked at 6.8 GB of memory.

The first LightGBM run on PaySim scored 0.0019: with frauds this rare the class weight made the scores saturate.
Capping the leaf output (`max_delta_step=1.0`) fixed it. Nine of the 18 features (the velocity and novelty ones)
carried no weight on PaySim, because PaySim has no device field and each account appears only a few times.

## Training on a GPU

`pip install -r requirements-train.txt` adds XGBoost, which is then trained and compared beside the other
models. `python ml/train.py --device cuda` (or `TRAIN_DEVICE=cuda`) trains it on an NVIDIA GPU; XGBoost's pip
package includes CUDA, so only the graphics driver is needed. The pip build of LightGBM is CPU only.
Each model's measured fit time is stored in `model_meta.json` under `fit_seconds`.

Measured on the author's laptop (NVIDIA RTX 5060 Laptop GPU, 8 GB), all 6.3 million PaySim rows,
`--skip random_forest,logistic_regression`, 15 Nov 2026:

| | XGBoost fit | Whole run | Peak memory | XGBoost test PR-AUC |
|---|---|---|---|---|
| CPU | 56.2 s | 172.3 s | 1.8 GB | 0.9820 |
| GPU (`--device cuda`) | 9.1 s | 124.3 s | 1.8 GB | 0.9819 |

The rest of the run is reading the CSV, building features and LightGBM (26 s), which stay on the CPU. On
500,000 rows the GPU saved under 2 seconds. The random forest is what made the earlier full run peak at
6.8 GB; skip it when memory is short.

Only model fitting moves to the GPU. Building the features, the other models and live scoring stay on the CPU;
live scoring handles one transaction at a time in well under a millisecond, which a GPU would not improve.
The scorer still serves LightGBM models only.

## Known weaknesses

- Never evaluated on real transactions.
- The score is an uncalibrated probability. 0.8 does not mean an 80% chance of fraud.
- New accounts: with no history the velocity features are empty. In a live test on simulated data, recall at
  the HIGH threshold was about 53% in the first minutes of a fresh stream against about 94% once accounts had history.
- `hour_of_day` barely varies in the simulated training data, so the model has learned nothing reliable from it.
- No fairness analysis has been done. The model uses no personal attributes, but amount and transaction patterns can
  stand in for them.
- Fraud changes. A model trained once goes stale.

## Monitoring

`GET /v1/model/drift` and the Performance tab compare the last 24 hours of live inputs (up to 20,000
transactions) with the training data, feature by feature, using the Population Stability Index: above 0.10
watch, above 0.25 drifted. Drift says the inputs changed. It does not say the model is wrong; only confirmed
labels do. Analysts' confirm and dismiss decisions are stored as labels for that purpose.

Not built: automatic retraining, alerting on drift, and tracking of precision and recall over time from labels.

## Before real use

1. Train on the deploying organisation's own labelled history, with their agreement and a lawful basis.
2. Evaluate on a later period the model has not seen, and agree the alert volume the review team can handle.
3. Run in shadow (scoring without acting) alongside the existing process and compare.
4. Have the model reviewed under the organisation's model-risk policy.
