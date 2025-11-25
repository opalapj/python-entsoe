import io
import os
import pathlib
import tomllib
import zipfile

import requests


# For the following regulation articles, the query response will contain
# data for a whole day (or a multiple of days, depending on the length
# of the requested time interval):
# 6.1.b,
# 12.1.b,
# 12.1.d,
# 12.1.f,
# 14.1.c,
# 14.1.d.

# For the following regulation articles, the query response will contain
# data for the requested MTU or BTU period(s):
# 6.1.a,
# 12.1.g,
# 16.1.a,
# 16.1.b&c,
# 17.1.d-h,
# 17.1.j.

TABLES = [
    "Actual Total Load [6.1.A]",
    "Day-ahead Total Load Forecast [6.1.B]",
    "Day-ahead Prices [12.1.D]",
    "Aggregated Generation per Type [16.1.B&C]",
    "Imbalance Prices [17.1.G]",
]

with pathlib.Path(os.getenv("ENTSOE_REFERENCE")).open("rb") as file:
    REFERENCE = tomllib.load(file)


def get_xml(table):
    params = {
        **REFERENCE[table],
        "securityToken": os.getenv("ENTSOE_API_KEY"),
        "periodStart": os.getenv("ENTSOE_START_DATETIME"),
        "periodEnd": os.getenv("ENTSOE_END_DATETIME"),
    }
    response = requests.get(url=os.getenv("ENTSOE_ENDPOINT"), params=params)
    print(response.headers["Content-Type"])
    if response.headers["Content-Type"] == "text/xml":
        print(response.text)
    elif response.headers["Content-Type"] == "application/octet-stream":
        with io.BytesIO(response.content) as in_memory_binary_stream:
            with zipfile.ZipFile(in_memory_binary_stream) as zip_file:
                content = zip_file.read(zip_file.filelist[0])
                print(content.decode())


def main():
    for table in TABLES:
        get_xml(table)


if __name__ == "__main__":
    main()
