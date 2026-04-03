from dataclasses import dataclass


@dataclass
class PropertyParams:
    property_price: float
    down_payment: float
    loan: float
    mortgage_rate: float
    mortgage_payment: float
    rent: float
    maintenance: float
    occupancy: float
    tax_rent: float
    tax_div: float
    fibra_mu: float
    fibra_sigma: float
    housing_mu: float
    housing_sigma: float
    inflation_mu: float
    inflation_sigma: float


params = PropertyParams(
    # property
    property_price=3750000,
    down_payment=1125000,
    loan=2625000,
    # credit
    mortgage_rate=0.109,
    mortgage_payment=30168,
    # rent
    rent=18000,
    maintenance=1000,
    occupancy=0.90,
    # taxes
    tax_rent=0.25,
    tax_div=0.30,
    # reit
    fibra_mu=0.09,
    fibra_sigma=0.20,
    # housing
    housing_mu=0.05,
    housing_sigma=0.03,
    # inflation
    inflation_mu=0.041,
    inflation_sigma=0.015,
)
