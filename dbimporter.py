import configparser
import MySQLdb


class DBConnector(object):

    def __init__(self, config):
        self.config = DBConfigLoader(config)

    def raw_connect(self, timeout=600):
        """
        create a db connection using MySQLdb.connect()

        db = MySQLdb.connect(
            host=config.get('database', 'server'),
            port=int(config.get('database', 'port')),
            user=config.get('database', 'username'),
            passwd=config.get('database', 'password'),
            db=config.get('database', 'name')
        )

        """
        conn = MySQLdb.connect(
            host=self.config.hostname,
            port=self.config.port,
            user=self.config.username,
            passwd=self.config.password,
            db=self.config.db
        )
        return conn

    def close(self):
        pass


class DBConfigLoader(object):
    """
    a config loader for the DBConnector class

    loads database connection configuration based on a configparser config object

    """

    def __init__(self, config):
        """
        db = MySQLdb.connect(
            host=config.get('database', 'server'),
            port=int(config.get('database', 'port')),
            user=config.get('database', 'username'),
            passwd=config.get('database', 'password'),
            db=config.get('database', 'name')
        )
        """
        self.config = config
        self.config_section = 'database'
        self.set_hostname()
        self.set_port()
        self.set_db()
        self.set_username()
        self.set_password()
        self.set_logfile()
        self.set_datastore()
        self.set_sqlalchemy_uri()

    def set_hostname(self):
        self.hostname = self.config.get(self.config_section, 'server')

    def set_port(self):
        self.port = int(self.config.get(self.config_section, 'port'))

    def set_db(self):
        self.db = self.config.get(self.config_section, 'name')

    def set_driver(self):
        self.driver = self.config.get(self.config_section, 'driver')

    def set_username(self):
        self.username = self.config.get(self.config_section, 'username')

    def set_password(self):
        self.password = self.config.get(self.config_section, 'password')

    def set_logfile(self):
        self.logfile = self.config.get('paths', 'logfile')

    def set_datastore(self):
        self.datastore = self.config.get('paths', 'datastore')

    def set_sqlalchemy_uri(self):
        self.sqlalchemy_uri = ''.join([
            'mysql',
            '+',
            'mysqldb',
            '://',
            self.username,
            ':',
            self.password,
            '@',
            self.hostname,
            ':',
            str(self.port),
            '/',
            self.db
        ])
