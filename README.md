# ETL_Assets_Fluctuations
Generates three random datasets about clients, assets and stocks information. Also, extract this datasets, make transformations to retrieve the assets value fluctuations from each client and. lastly, save these assets value fluctuations as a CSV

# Instruction
### Generating the test data

```bash
python3 -m data_generator.clients_generator clients_entries
python3 -m data_generator.stocks_generator stocks_entries
python3 -m data_generator.assets_generator assets_entries
```
where:
- clients_entries: is an integer representing the amount of unique clients you want to be generated
- stocks_entries: is an integer representing the number of days, from today to the past, where we have stocks information
- assets_entries: is an integer representing the amount of assets, randomly assigned to clients, you want to be generated

### Run the ETL
```bash
$SPARK_HOME/bin/spark-submit \
    --py-files packages.zip \
    --files configs/assets_value_fluctuations_job_config.json \
    jobs/assets_value_fluctuations_job.py
```

### Test the ETL
```bash
python3 test_assets_value_fluctuations_job.py
```
