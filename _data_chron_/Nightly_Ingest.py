from typing import NamedTuple, Dict, List
import os
import re
import urllib3
import certifi
import mysql.connector
from dotenv import load_dotenv
from Nighty_Ingest_Playoffs import NightlyIngestPlayoffs
from Nighty_Ingest_Regular_Season import NightlyIngestRegularSeason

load_dotenv()

MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
URL_NIGHTLY_PLAYER_TOTALS = os.getenv('URL_NIGHTLY_PLAYER_TOTALS')
URL_CUMULATIVE_PLAYER_STATS = os.getenv('URL_CUMULATIVE_PLAYER_STATS')

class Endpoint(NamedTuple):
    """Typing for self.endpoint"""
    url: str
    mapping: Dict[str, str]
    table_name: str
    column_widths: List[int]

class NightlyIngest:
    """Methods to deal with the ingesting of nightly stats: usually released around 1am"""

    def __init__(self) -> None:
        self.starting_num_records = 0
        self.ending_num_records = 0
        self.sql_pool = None
        self.create_connection_pool()
        self.endpoints = [
            {'url': URL_NIGHTLY_PLAYER_TOTALS,
             'table_name': 'nightly_player_totals'
             },
            {'url': URL_CUMULATIVE_PLAYER_STATS,
             'table_name': 'cumulative_player_stats'
             }
        ]

    def create_connection_pool(self):
        """Method to create a connection pool"""
        config = {
            'host': MYSQL_HOST,
            'port': MYSQL_PORT,
            'user': MYSQL_USER,
            'database': MYSQL_DATABASE,
            'password': MYSQL_PASSWORD,
            'pool_name': 'player_stats_connection_pool',
            'pool_size': 10
        }
        try:
            self.sql_pool = mysql.connector.pooling.MySQLConnectionPool(
                **config)
            print("\n" + "\033[0;92m" +
                  "CONNECTION POOL CREATED:", self.sql_pool, "\033[0m")
        except mysql.connector.Error as err:
            if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
                print('Something is wrong with your username or password')
            elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
                print(f'Database {config["database"]} does not exist')
            else:
                print(err)

    def get_set_endpoint_data(self):
        """Method to get .txt data from all endpoints within `self.endpoints`"""
        headers = {'User-Agent':
                   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
                   }
        http = urllib3.PoolManager(
            10, headers, cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
        for endpoint in self.endpoints:
            response = http.request('GET', endpoint['url'])
            content = response.data.decode('utf-8')
            self.process_data_for_sql(content, endpoint)
        return self

    def csv_local_file_upload(self, file: str):
        """Method to handle the inclusion of csv"""
        with open(file, 'r', 1, 'utf-8') as file_data:
            next(file_data)
            content = file_data.read()

            self.process_data_for_sql(content, self.endpoints[0])
        return self

    def get_record_count(self, table_name: str) -> int:
        """Method to get the current number of records in the nightly player table"""
        connection = self.sql_pool.get_connection()
        cursor = connection.cursor()
        query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(query)
        return int(cursor.fetchone()[0])

    def process_data_for_sql(self, content: str, endpoint_data: Endpoint):
        """Method that processed the data, for SQL inclusion"""
        self.starting_num_records = self.get_record_count(endpoint_data['table_name'])
        connection = self.sql_pool.get_connection()
        cursor = connection.cursor()
        playoff_regex_pattern = r'PLAYOFFS / INCLUDES GAMES OF .*|\n\n+'

        # Cumulative Stats - Broken out between regular season and playoffs
        if endpoint_data['table_name'] == 'cumulative_player_stats':
            pages = content.split('\n\n\n\n')
            cursor.execute('TRUNCATE TABLE cumulative_player_stats')
            for page in pages:
                if not page.strip():
                    continue
                if re.match(playoff_regex_pattern, page):
                    page = re.sub(playoff_regex_pattern, '', page)
                    NightlyIngestPlayoffs().cumulative(page)
                else:
                    connection.commit()
                    page = re.sub(r'\n\n', '', page)
                    NightlyIngestRegularSeason().cumulative(page)
        else:
            # if we're nightly stats
            pages = content.split('\n\n\n\n')
            for page in pages:
                if not page.strip():
                    continue
                if re.match(playoff_regex_pattern, page):
                    page = re.sub(playoff_regex_pattern, '', page)
                    NightlyIngestPlayoffs().nightly(page)
                else:
                    page = re.sub(r'\n\n', '', page)
                    NightlyIngestRegularSeason().nightly(page)

    def close_connection(self):
        """Method to close database connection"""
        connection = self.sql_pool.get_connection()
        connection.close()
        print("\n" + "\033[1;92m" +
              "CONNECTION CLOSED" + "\033[0m")
        return self
