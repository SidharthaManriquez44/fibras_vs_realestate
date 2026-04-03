import numpy as np
from src.fibras_vs_realestate.config.initial_param import PropertyParams


def simulate_property(params: PropertyParams):
    value = params.property_price
    debt = params.loan
    cashflow = 0

    for year in range(20):
        appreciation = np.random.normal(params.housing_mu, params.housing_sigma)

        value *= 1 + appreciation

        annual_rent = params.rent * 12 * params.occupancy

        net_rent = annual_rent - params.maintenance * 12

        net_rent *= 1 - params.tax_rent

        cashflow += net_rent

    equity = value - debt

    return equity + cashflow
