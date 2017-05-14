import configparser
import MySQLdb
import pandas as pd
import glob
from dbimporter import *
from sqlalchemy import create_engine


def bulk_csv_to_sql(csvfiles, engine):
    """
    TODO:
    - does the con need to be closed? dont know how
    - make this generic, not coupled with alarm table
    - move to class
    """
    for csvfile in csvfiles:
        csv_to_sql(csvfile, engine)

def csv_to_sql(csvfile, engine):
    """
    TODO:
    IMPORTANT
    the entire insert will fail to commit if duplicate primary keys
    need to write another function to try smaller inserts if fails due to duplicates
    have manually replaced df with df.ix[2:-2] manually
    """
    df = pd.read_csv(csvfile)
    # append site_id column
    site_col = pd.Series([site_id]*len(df))
    df = df.assign(site_id=site_col)
    # insert into db table
    try:
        df.ix[2:-2].to_sql(con=engine, name='alarm', if_exists='append', index=False)
        print("Success!")
    except Exception as e:
        print("Failed!")
        print(str(e)[:100])


if __name__ == '__main__':

    # import config
    config = configparser.ConfigParser()
    master_config_file = 'config.ini'
    config.read(master_config_file)
    database_config_file = config.get('paths', 'database_config')
    config.read(database_config_file)

    # test connection
    # db = MySQLdb.connect(
    #     host=config.get('database', 'server'),
    #     port=int(config.get('database', 'port')),
    #     user=config.get('database', 'username'),
    #     passwd=config.get('database', 'password'),
    #     db=config.get('database', 'name')
    # )
    connector = DBConnector(config)
    connection = connector.raw_connect()
    cursor = connection.cursor()
    # cursor.execute("""
    #     SELECT *
    #     FROM alarm
    # """)
    # result = cursor.fetchall()
    # print(result)

    connection.close()
    # connect using sqlalchemy
    engine = create_engine(connector.config.sqlalchemy_uri)

    site_id = 3
    # read csv
    datastore = config.get('paths', 'datastore')
    csvfiles = glob.glob(datastore + '/*.csv')
    # read csv into df
    # insert into db table
    bulk_csv_to_sql(csvfiles, engine)
    # df.to_sql(con=engine, name='alarm', if_exists='append', index=False)
    # connection.commit()
