# medusa-data-importer
import site data into medusa db

the script runs as medusa-data-import.service, using a loop.
csv files are downloaded from the medusa-site-data S3 bucket, imported into the medusa database, then deleted on the bucket.
these must be in folders with the naming structure: SITEID_xxxxx

config settings are taken from the same file as medusa
