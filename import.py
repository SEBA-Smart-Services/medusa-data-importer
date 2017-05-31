import configparser
import os
import ast
import boto3
import csv
import time
import logging
from sqlalchemy import create_engine, select
from sqlalchemy.schema import MetaData, Table

# choose which config file to load
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

# configure log file
logging.basicConfig(filename=config_dict['logfile'], level=logging.INFO, format='%(asctime)s %(message)s')
logging.info("Medusa data importer initialised")

# connect using sqlalchemy
engine = create_engine(config_dict['SQLALCHEMY_DATABASE_URI'])
conn = engine.connect()
meta = MetaData(bind=engine)
logged_entity_table = Table('logged_entity', meta, autoload=True)
log_time_value_table = Table('log_time_value', meta, autoload=True)
alarm_table = Table('alarm', meta, autoload=True)

# configure s3 connection
s3 = boto3.resource('s3')

# main loop
while True:
    # iterate through files in bucket
    files = s3.meta.client.list_objects_v2(Bucket=config_dict['s3_bucket'])
    # if there are no files, then contents key will not exist
    if 'Contents' in files:
        filenames = [file['Key'] for file in files['Contents']]

        for filename in filenames:
            try:
                # check if the file is valid
                is_csv = filename.split('.')[-1] == 'csv'
                has_site_id = filename.split('_')[0].isdigit()

                if is_csv and has_site_id:
                    # remove the folder path from filename
                    filename_local = filename.split('/')[-1]

                    # find which table it should be inserted into
                    filename_local_parts = filename_local.split('_')
                    if filename_local_parts[1] == 'tbLoggedEntities':
                        table = logged_entity_table
                        pk_col1 = logged_entity_table.c.ID
                        pk_col2 = None
                        table_valid = True
                    elif filename_local_parts[1] == 'tbLogTimeValues':
                        table = log_time_value_table
                        pk_col1 = log_time_value_table.c.SeqNo
                        pk_col2 = log_time_value_table.c.ParentID
                        table_valid = True
                    elif filename_local_parts[1] == 'tbAlarmsEvents':
                        table = alarm_table
                        pk_col1 = alarm_table.c.SeqNo
                        pk_col2 = None
                        table_valid = True
                    else:
                        table_valid = False

                    if table_valid:
                        logging.info("Importing file {}".format(filename))

                        # download csv from S3 and open with a csv dict reader
                        s3.meta.client.download_file(config_dict['s3_bucket'], filename, filename_local)
                        csvfile = open(filename_local)
                        reader = csv.DictReader(csvfile)

                        # get site id from folder name prefix
                        site_id = int(filename.split('_')[0])

                        # load ids of existing entries in db. select all rows from table, filter by site id, then select only primary keys (as a tuple)
                        s = select([pk_col1, pk_col2]).where(table.c.site_id == site_id)
                        result = conn.execute(s)
                        rows = result.fetchall()
                        # put into a set for more efficient lookups
                        primary_keys = set([row[:] for row in rows])

                        # build list of rows to add. only include a row if it doesn't already exist in db
                        to_add = []
                        for row in reader:
                            # assemble the tuple of primary keys to search for
                            pk1 = int(row[pk_col1.name])
                            if pk_col2 is None:
                                pk2 = None
                            else:
                                pk2 = int(row[pk_col2.name])

                            if not (pk1, pk2) in primary_keys:
                                row['site_id'] = site_id
                                to_add.append(row)

                        # insert to db
                        if len(to_add) > 0:
                            conn.execute(table.insert(), to_add)

                        # delete file locally
                        os.remove(filename_local)

                        # delete file on s3 bucket
                        s3.meta.client.delete_object(Bucket=config_dict['s3_bucket'], Key=filename)

            except Exception as e:
                logging.error("Exception {} when trying to import {}".format(e, filename))

    # sleep
    time.sleep(config_dict['loop_seconds'])
