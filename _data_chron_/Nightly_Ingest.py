from typing import NamedTuple, Dict, List
import os
from io import StringIO
import re
from mysql.connector import errorcode
import urllib3
import certifi
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

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
        self.sql_pool = None
        self.create_connection_pool()
        self.endpoints = [
            {'url': URL_NIGHTLY_PLAYER_TOTALS,
             'columns': ['date', 'team', 'opponent', 'player_name', 'position',
                         'games_played', 'minutes_played', 'field_goals',
                         'field_goal_attempts', 'three_point_field_goals',
                         'three_point_field_goal_attempts', 'free_throws',
                         'free_throw_attempts', 'offensive_rebounds', 'defensive_rebounds',
                         'total_rebounds', 'assists', 'personal_fouls', 'disqualifications',
                         'steals', 'turnovers', 'blocks', 'points_scored'],
             'table_name': 'nightly_player_totals',
             'column_widths': [11, 4, 4, 25, 7, 2, 5, 3, 4, 4, 5,
                               3, 4, 4, 4, 4, 5, 4, 3, 5, 3, 4, 4],
             },
            {'url': URL_CUMULATIVE_PLAYER_STATS,
             'columns': ["SCOPE", "TEAM", "RS", "NAME_TEAM", "GAMES_PLAYED", "GAMES_STARTED",
                         "TOT_MIN", "FG", "FGA", "FG_PCT", "FG3", "FG3A", "FG3_PCT",
                         "FT", "FTA", "FT_PCT", "OFF_REB", "DEF_REB", "TOT_REB", "AST",
                         "PF", "DQ", "ST", "TR_OVR", "BLKS", "TOT_PTS", "PPG", "HIGH"],
             'table_name': 'cumulative_player_stats',
             'column_widths': [6, 4, 4, 38, 3, 3, 6, 5, 6, 6, 5, 5, 6,
                               5, 5, 6, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 5, 3]
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
            'pool_size': 5
        }
        try:
            self.sql_pool = mysql.connector.pooling.MySQLConnectionPool(
                **config)
            print("\n" + "\033[0;92m" +
                  "CONNECTION POOL CREATED:", self.sql_pool, "\033[0m")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print('Something is wrong with your username or password')
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
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

    def process_data_for_sql(self, content: str, endpoint_data: Endpoint):
        """Method that processed the data, for SQL inclusion"""
        connection = self.sql_pool.get_connection()
        cursor = connection.cursor()
        columns = endpoint_data['columns']
        pages = content.split('\n\n\n\n')
        regex_pattern = r'INCLUDES GAMES OF .*|\n\n+'

        for page in pages:
            # If page is empty
            if not page.strip():
                continue
            # Remove the INCLUDED GAMES... heading
            page = re.sub(regex_pattern, '', page)

            # To pass it into pandas fixed width, we need to convert to memory buffer
            data_buffer = StringIO(page)
            # Read data as fixed width based on column widths

            data_frame = pd.read_fwf(
                data_buffer,
                widths=endpoint_data['column_widths'],
                skiprows=1,
                names=columns)

            if endpoint_data['table_name'] == 'nightly_player_totals':
                data_frame['date'] = pd.to_datetime(
                    data_frame['date'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')

            # # Convert DataFrame to Dict
            data_dict = data_frame.to_dict(orient='records')

            for row in data_dict:

                # For table_name, the name is intertwined with the name.
                # A future improvement could be to break out the team[s] into another column.
                if endpoint_data['table_name'] == 'cumulative_player_stats':
                    name_list = row['NAME_TEAM'].split(',')
                    first_and_end = name_list[1].strip().split(' ')
                    name_joined = ''
                    if len(first_and_end) == 2:
                        name_joined = f"{first_and_end[0]} {name_list[0]} {first_and_end[1]}"
                    else:
                        name_joined = f"{name_list[1].strip()} {name_list[0]}"
                    query = f'SELECT id FROM active_players WHERE full_name="{name_joined}" AND is_active=1'
                    cursor.execute(query)
                    player_id = cursor.fetchall()
                    if len(player_id) == 0:
                        query = f'SELECT id FROM active_players WHERE first_name="{first_and_end[0]}" AND last_name="{name_list[0]}" AND is_active=1'
                        cursor.execute(query)
                        player_id = cursor.fetchall()

                    # tuple in array
                    row['PLAYER_ID'] = player_id[0][0]

                # For nightly player totals
                elif endpoint_data['table_name'] == 'nightly_player_totals':
                    # We derive a id from date and player name so it's unique
                    split_name = row['player_name'].split(',')
                    _first = split_name[1].split()[0]
                    _last = split_name[0].split()[0]
                    query = f'SELECT id FROM active_players WHERE first_name="{_first}" AND last_name="{_last}" AND is_active=1'
                    cursor.execute(query)

                    player_id = cursor.fetchall()
                    # Check for empty ids
                    # print(player_id,_first,_last)
                    if len(player_id):
                        player_id = player_id[0][0]
                    else:
                        player_id = -1
                        print('MISSING_KEY')
                    _id = row['date'].replace('-', '') + str(player_id)
                    row['id'] = _id

                keys = ', '.join(row.keys())
                values = tuple(row.values())
                if endpoint_data['table_name'] == 'cumulative_player_stats':
                    query = f"REPLACE INTO {endpoint_data['table_name']} ({keys}) VALUES {values}"
                else:
                    query = f"INSERT IGNORE INTO {endpoint_data['table_name']} ({keys}) VALUES {values}"
                # Insertion in DB
                try:
                    cursor.execute(query)
                except (TimeoutError, ConnectionError, TypeError) as err:
                    print("\033[0;91m" +
                          'There has been an ERROR:', err + "\033[0m")

        connection.commit()
        return self

    def close_connection(self):
        """Method to close database connection"""
        connection = self.sql_pool.get_connection()
        connection.close()
        print("\n" + "\033[1;92m" +
              "CONNECTION CLOSED" + "\033[0m")
        return self


# NI = NightlyIngest()
# NI.get_set_endpoint_data().close_connection()
