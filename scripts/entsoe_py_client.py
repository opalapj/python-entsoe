import logging
import os

import entsoe as tp
import pandas as pd


COLUMNS_MAPPER = {
    "Dayahead Price": "day ahead price [PLN/MWh]",
    "Imbalance Price": "imbalance price [PLN/MWh]",
    "Forecasted Load": "load forecast [MWh]",
    "Actual Load": "load actual [MWh]",
}

COMMON_PARAMS = {
    "start": pd.Timestamp(
        os.getenv("ENTSOE_START_DATETIME"), tz=os.getenv("ENTSOE_TZ")
    ),
    "end": pd.Timestamp(os.getenv("ENTSOE_END_DATETIME"), tz=os.getenv("ENTSOE_TZ")),
    "country_code": os.getenv("ENTSOE_COUNTRY_CODE"),
}


def show_logs():
    logging.basicConfig(level=logging.NOTSET)


def get_day_ahead_prices(client):
    day_ahead_prices = client.query_day_ahead_prices(**COMMON_PARAMS)
    # Additionally aggregation.
    day_ahead_prices = day_ahead_prices.rename("Dayahead Price")
    # Query returns set with end date record.
    day_ahead_prices = day_ahead_prices.iloc[:-1]
    return day_ahead_prices


def get_imbalance_prices(client):
    imbalance_prices = client.query_imbalance_prices(**COMMON_PARAMS)
    # Additionally aggregation.
    imbalance_prices = imbalance_prices.loc[:, "Long"]
    imbalance_prices = imbalance_prices.rename("Imbalance Price")
    return imbalance_prices


def get_load_forecast(client):
    return client.query_load_forecast(**COMMON_PARAMS)


def get_load(client):
    return client.query_load(**COMMON_PARAMS)


def get_generation(client):
    return client.query_generation(**COMMON_PARAMS)


def get_summary(*tables):
    summary = pd.concat(tables, axis=1)
    summary.index.rename("date [-]", inplace=True)
    summary.rename(columns=COLUMNS_MAPPER, inplace=True)
    return summary


def to_csv_database_columns(table):
    table.columns = (
        table.columns.str.partition("[").levels[0].str.strip().str.replace(" ", "_")
    )
    table = table.rename_axis(index="datetime")
    table.to_csv(os.getenv("ENTSOE_CSV_LIKE_DB_PATH"))


def main():
    show_logs()
    client = tp.EntsoePandasClient(os.getenv("ENTSOE_API_KEY"))
    day_ahead_prices = get_day_ahead_prices(client)
    imbalance_prices = get_imbalance_prices(client)
    load_forecast = get_load_forecast(client)
    load = get_load(client)
    generation = get_generation(client)
    summary = get_summary(day_ahead_prices, imbalance_prices, load_forecast, load)
    summary.to_csv(os.getenv("ENTSOE_CSV_PATH"))
    to_csv_database_columns(summary)


if __name__ == "__main__":
    main()
