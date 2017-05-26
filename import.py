import configparser
import os
import ast
import boto3
import csv
import time
from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData, Table

# choose which part of the config file to load
config_file = os.getenv('MEDUSA_CONFIG', '/var/lib/medusa/medusa-development.ini')

# configparser handles everything as strings so some additional conversion work is needed
config = configparser.ConfigParser()
# prevent configparser from converting to lowercase
config.optionxform = str
# read in config from ini
config.read(config_file)
config_dict = {}

# remove jobs value, because it has newlines and breaks literal_eval
config['flask'].pop('JOBS')

# load values
# convert the strings to python objects
for key in config['flask']:
    config_dict[key] = ast.literal_eval(config['flask'][key])

for key in config['data-importer']:
    config_dict[key] = ast.literal_eval(config['data-importer'][key])

# connect using sqlalchemy
engine = create_engine(config_dict['SQLALCHEMY_DATABASE_URI'])
conn = engine.connect()
meta = MetaData(bind=engine)
alarms_table = Table('alarm', meta, autoload=True)

# main loop
while True:
    # iterate through files in bucket
    s3 = boto3.resource('s3')
    files = s3.meta.client.list_objects_v2(Bucket=config_dict['s3_bucket'])
    filenames = [file['Key'] for file in files['Contents']]

    for filename in filenames:
        is_csv = filename.split('.')[-1] == 'csv'
        has_site_id = filename.split('_')[0].isdigit()

        if is_csv and has_site_id:
            # remove the folder path from filename
            filename_local = filename.split('/')[-1]

            # download csv from S3 and open with a csv dict reader
            s3.meta.client.download_file(config_dict['s3_bucket'], filename, filename_local)
            csvfile = open(filename_local)
            reader = csv.DictReader(csvfile)

            # get site id from folder name prefix
            site_id = int(filename.split('_')[0])

            # load ids of existing entries in db. select all rows from table, filter by site id, then select only SeqNos
            from sqlalchemy.sql import select
            s = select([alarms_table.c.SeqNo]).where(alarms_table.c.site_id == site_id)
            result = conn.execute(s)
            rows = result.fetchall()
            # put into a set for more efficient lookups
            seqnos = set([row[0] for row in rows])

            # build list of rows to add. only include a row if it doesn't already exist in db
            to_add = []
            for row in reader:
                if not int(row['SeqNo']) in seqnos:
                    row['site_id'] = site_id
                    to_add.append(row)

            # insert to db
            if len(to_add) > 0:
                conn.execute(alarms_table.insert(), to_add)

            # delete file locally
            os.remove(filename_local)

            # delete file on s3 bucket
            s3.meta.client.delete_object(Bucket=config_dict['s3_bucket'], Key=filename)

    # sleep
    time.sleep(config_dict['loop_seconds'])
