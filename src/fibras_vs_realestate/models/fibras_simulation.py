import numpy as np
from src.fibras_vs_realestate.config.initial_param import PropertyParams


def simulate_fibra_portfolio(initial, monthly, months, params: PropertyParams):
    value = initial

    for m in range(months):
        r = np.random.normal(params.fibra_mu / 12, params.fibra_sigma / np.sqrt(12))

        value = value * (1 + r)

        dividend = value * 0.08 / 12

        dividend = dividend * (1 - params.tax_div)

        value += dividend

        value += monthly

    return value


def calculate_statistics(df):
    stats = df.groupby("ticker")["log_return"].agg(["mean", "std"]).reset_index()

    stats.columns = ["ticker", "mu", "sigma"]

    return stats
