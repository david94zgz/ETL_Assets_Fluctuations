"""
stocks_generator.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the code necessary to generate the datasets that will be used later on
in the pipeline at the stocks_value_fluctuations_job.py module. It uses the configurations from 
the stocks_config.json file.
The dataset consist of records about the date, stock_price, kind of news and interes rate.
"""

import json
import random
import sys
from datetime import date, timedelta


def gen_stocks_dataset(stocks_entries: int):
    """Produces a stocks dataset which will be saved in the 'data' directory.
    """
    path_to_stocks_config_file = "configs/stocks_config.json"
    with open(path_to_stocks_config_file, "r") as stocks_config_file:
        stocks_config_dict = json.load(stocks_config_file)

    with open(stocks_config_dict["stocks_dataset_fn"], "w") as stocks_dataset_file:
        stocks_list = []
        for i in range(0, stocks_entries):
            stocks_dictionary = {}
            stocks_dictionary[stocks_config_dict["stocks_fieldnames"][0]] = (date.today() - timedelta(i)).strftime("%Y-%m-%d")
            stocks_dictionary[stocks_config_dict["stocks_fieldnames"][1]] = 50000 + (i*random.randint(-1000,1000))
            stocks_dictionary[stocks_config_dict["stocks_fieldnames"][2]] = random.choice(stocks_config_dict["news"])
            stocks_dictionary[stocks_config_dict["stocks_fieldnames"][3]] = "%.4f" % (0.02 + round(random.uniform(-0.025, 0.025), 4))
            stocks_list.append(stocks_dictionary)
        stocks_json = json.dumps(stocks_list, indent=4)
        stocks_dataset_file.write(stocks_json)
    print("Wrote {} lines in {} file".format(stocks_entries, stocks_config_dict["stocks_dataset_fn"]))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: stocks_generator stocks_entries", file=sys.stderr)
        sys.exit(-1)
    
    stocks_entries = int(sys.argv[1])
    gen_stocks_dataset(stocks_entries)