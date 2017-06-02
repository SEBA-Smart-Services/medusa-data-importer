# medusa-data-importer
Background service that imports data into the Medusa DB from S3.

The python script runs as `medusa-data-import.service`, using a loop with a configurable delay time.

CSV files are downloaded from the medusa-site-data S3 bucket, imported into the medusa database, then deleted on the bucket.
These must be in folders with the naming structure: `SITEID_xxxxx` and filename `SITEID_TABLENAME_xxxxx.csv`.

Config settings are taken from the same files as medusa, and are downloaded before running the service in the same manner (via medusa-config.service)
