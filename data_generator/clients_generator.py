"""
clients_generator.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the code necessary to generate the datasets that will be used later on
in the pipeline at the clients_value_fluctuations_job.py module. It uses the configurations from 
the clients_config.json file.
The dataset consist of records about the Id, name and surname of the client.
"""

import json
import csv
import random
import sys


def gen_clients_dataset(clients_entries: int) -> csv:
        """Produces a clients dataset which will be saved in the 'data' directory.
        """
        path_to_clients_config_file = "configs/clients_config.json"
        with open(path_to_clients_config_file, "r") as clients_config_file:
                clients_config_dict = json.load(clients_config_file)
                clients_config_dict.update({
                        "names_and_surnames": [(name, surname) for name in clients_config_dict["names"] 
                                                                for surname in clients_config_dict["surnames"]]
                        })
        
        with open(clients_config_dict["clients_dataset_fn"], 'w') as clients_dataset_file:
                dataset_writer = csv.writer(clients_dataset_file, delimiter=",", quoting=csv.QUOTE_MINIMAL)
                dataset_writer.writerow(clients_config_dict["clients_fieldnames"])
                random_names_and_surnames = random.sample(clients_config_dict["names_and_surnames"], clients_entries)
                for i in range(0, clients_entries):
                        dataset_writer.writerow([i+1, random_names_and_surnames[i][0], random_names_and_surnames[i][1]])
        print("Wrote {} lines in {} file".format(clients_entries, clients_config_dict["clients_dataset_fn"]))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: clients_generator clients_entries", file=sys.stderr)
        sys.exit(-1)
    
    clients_entries = int(sys.argv[1])
    gen_clients_dataset(clients_entries)