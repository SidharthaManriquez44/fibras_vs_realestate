from fibras_vs_realestate.config.path.path_builder import DataLakePathBuilder
from fibras_vs_realestate.config.storage.data_save import DataSaver


def save_parquet(df, layer, domain, dataset, execution_date=None):
    path = f"s3://raw/{domain}/prices_raw/{execution_date}.parquet"

    datalake = DataLakePathBuilder(path)
    saver = DataSaver(datalake, execution_date)

    return saver.save_parquet(layer=layer, domain=domain, dataset=dataset, df=df)
