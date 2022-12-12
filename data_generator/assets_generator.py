"""
assets_generator.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the code necessary to generate the datasets that will be used later on
in the pipeline at the assets_value_fluctuations_job.py module. It uses the configurations from 
the assets_config.json file.
The dataset consist of records about the Id of the asset, Id of the client owning the
asset, kind of asset, market value and date of the market value.
"""


import json
import csv
import random
import sys
import os
from datetime import date, timedelta


def gen_assets_dataset(assets_entries: int):
    """Produces a assets dataset which will be saved in the 'data' directory.
    """
    # Take the configurations and the clients and stocks entries
    path_to_assets_config_file = "configs/assets_config.json"
    with open(path_to_assets_config_file, "r") as assets_config_file:
        assets_config_dict = json.load(assets_config_file)
    
    path_to_clients_dataset_file = "data/clients_dataset.csv"
    with open(path_to_clients_dataset_file, "r") as clients_dataset_file:
        clients_entries = sum(1 for row in clients_dataset_file)
    
    path_to_stocks_dataset_file = "data/stocks_dataset.json"
    with open(path_to_stocks_dataset_file, "r") as stocks_dataset_file:
        stocks_entries = len(json.load(stocks_dataset_file))

    # Produce the assets dataset
    with open(assets_config_dict["assets_dataset_fn"], "w") as assets_dataset_file:
        dataset_writer = csv.writer(assets_dataset_file, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        dataset_writer.writerow(assets_config_dict["assets_fieldnames"])
        for i in range(0, assets_entries):
            assets = random.choice(assets_config_dict["assets"])
            if assets != "Stocks":
                dataset_writer.writerow([i+1, random.randint(1, clients_entries-1), assets, random.randint(100, 100000), (date.today() - timedelta(random.randint(1, stocks_entries))).strftime("%Y-%m-%d")])
            else:
                dataset_writer.writerow([i+1, random.randint(1, clients_entries-1), assets, None, (date.today() - timedelta(random.randint(1, stocks_entries))).strftime("%Y-%m-%d")])
    
    print("Wrote {} lines in {} file".format(assets_entries, assets_config_dict["assets_dataset_fn"]))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: assets_generator assets_entries", file=sys.stderr)
        sys.exit(-1)
    elif not os.path.exists("data/clients_dataset.csv") or not os.path.exists("data/stocks_dataset.json"):
        print("Error: clients_dataset and/or stocks_dataset missing", file=sys.stderr)
        sys.exit(-1)
        
    assets_entries = int(sys.argv[1])
    gen_assets_dataset(assets_entries)