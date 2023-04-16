from io import StringIO
import os
import pandas as pd
from dotenv import load_dotenv
import mysql.connector
load_dotenv()

MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
URL_NIGHTLY_PLAYER_TOTALS = os.getenv('URL_NIGHTLY_PLAYER_TOTALS')
URL_CUMULATIVE_PLAYER_STATS = os.getenv('URL_CUMULATIVE_PLAYER_STATS')


class NightlyIngestRegularSeason:
    """ Class to handle ingest of playoff data """

    def __init__(self):
        self.sql_pool = None
        self.create_connection_pool()
        self.nightly_col_widths = [11, 4, 4, 25, 7, 2, 5, 3, 4, 4, 5, 3, 4, 4, 4, 4, 5, 4, 3, 5, 3, 4, 4]
        self.nightly_col_names = ['date', 'team', 'opponent', 'player_name', 'position',
                                  'games_played', 'minutes_played', 'field_goals',
                                  'field_goal_attempts', 'three_point_field_goals',
                                  'three_point_field_goal_attempts', 'free_throws',
                                  'free_throw_attempts', 'offensive_rebounds', 'defensive_rebounds',
                                  'total_rebounds', 'assists', 'personal_fouls', 'disqualifications',
                                  'steals', 'turnovers', 'blocks', 'points_scored']
        self.cumulative_col_widths = [6, 4, 4, 38, 3, 3, 6, 5, 6,
                                      6, 5, 5, 6, 5, 5, 6, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 5, 3]
        self.cumulative_col_names = ["SCOPE", "TEAM", "RS", "NAME_TEAM", "GAMES_PLAYED", "GAMES_STARTED",
                                     "TOT_MIN", "FG", "FGA", "FG_PCT", "FG3", "FG3A", "FG3_PCT",
                                     "FT", "FTA", "FT_PCT", "OFF_REB", "DEF_REB", "TOT_REB", "AST",
                                     "PF", "DQ", "ST", "TR_OVR", "BLKS", "TOT_PTS", "PPG", "HIGH"]

    def nightly(self, page):
        """ Method to handle the importing of nightly playoff data """
        connection = self.sql_pool.get_connection()
        cursor = connection.cursor()
        # To pass it into pandas fixed width, we need to convert to memory buffer
        data_buffer = StringIO(page)
        # Read data as fixed width based on column widths
        data_frame = pd.read_fwf(
            data_buffer,
            widths=self.nightly_col_widths,
            skiprows=1,
            names=self.nightly_col_names)

        # Convert date to YYYY-MM-DD
        data_frame['date'] = pd.to_datetime(data_frame['date'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')

        # Convert DataFrame to Dict
        data_dict = data_frame.to_dict(orient='records')

        for row in data_dict:
            split_name = row['player_name'].split(',')
            _first = split_name[1].split()[0]
            _last = split_name[0].split()[0]
            query = f'SELECT id FROM active_players WHERE first_name="{_first}" AND last_name="{_last}" AND is_active=1'
            cursor.execute(query)
            player_id = cursor.fetchall()
            if len(player_id):
                player_id = player_id[0][0]
            else:
                player_id = -1
                print(f'Missing Player Key (Playoffs) for {_first} {_last} - Please add to active_players table')
            _id = row['date'].replace('-', '') + str(player_id)
            row['id'] = _id
            derived_stats = self.derive_stats(row)
            row['dbl_trpl_double'] = derived_stats['dbl_trpl_double']
            row['field_goal_percentage'] = derived_stats['field_goal_percentage']
            row['free_throw_percentage'] = derived_stats['free_throw_percentage']
            row['three_point_percentage'] = derived_stats['three_point_percentage']
            row['assist_to_turnover'] = derived_stats['assist_to_turnover']
            keys = ', '.join(row.keys())
            values = tuple(row.values())
            query = f"INSERT IGNORE INTO playoffs_nightly_player_totals ({keys}) VALUES {values}"
            print(query)
            # Insertion in DB
            try:
                cursor.execute(query)
            except (TimeoutError, ConnectionError, TypeError) as err:
                print("\033[0;91m" + 'There has been an ERROR:', err + "\033[0m")
        connection.commit()

    def cumulative(self, page):
        """ Method to handle the importing of cumulative playoff data"""
        connection = self.sql_pool.get_connection()
        cursor = connection.cursor()
        # To pass it into pandas fixed width, we need to convert to memory buffer
        data_buffer = StringIO(page)
        # Read data as fixed width based on column widths
        data_frame = pd.read_fwf(
            data_buffer,
            widths=self.cumulative_col_widths,
            skiprows=1,
            names=self.cumulative_col_names)
        data_dict = data_frame.to_dict(orient='records')
        for row in data_dict:
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
            if len(player_id):
                player_id = player_id[0][0]
            else:
                player_id = -1
                print(f'Missing Player Key (Reg Season - Cume) for {first_and_end[0]} {name_list[0]} - Please add to active_players table')
            row['id'] = player_id
            keys = ', '.join(row.keys())
            values = tuple(row.values())
            query = f"INSERT IGNORE INTO cumulative_player_stats ({keys}) VALUES {values}"
            # Insertion in DB
            try:
                cursor.execute(query)
            except (TimeoutError, ConnectionError, TypeError) as err:
                print("\033[0;91m" + 'There has been an ERROR:', err + "\033[0m")
        connection.commit()

    def derive_stats(self, row):
        """ Method to derive double/triple double, fg%, 3pt% ft%, assist/turnover"""
        dbl_trpl_double = 'NULL'
        field_goal_percentage = 'NULL'
        free_throw_percentage = 'NULL'
        three_point_percentage = 'NULL'
        assist_to_turnover = 'NULL'
        if row['field_goal_attempts'] > 0:
            field_goal_percentage = round(row['field_goals'] / row['field_goal_attempts'], 2)
        if row['three_point_field_goal_attempts'] > 0:
            three_point_percentage = round(row['three_point_field_goals'] / row['three_point_field_goal_attempts'], 2)
        if row['free_throw_attempts'] > 0:
            free_throw_percentage = round(row['free_throw_attempts'] / row['free_throw_attempts'], 2)
        if row['turnovers'] > 0:
            assist_to_turnover = round(row['assists'] / row['turnovers'], 2)
        temp = 0
        double_test = ['assists', 'blocks', 'points_scored', 'total_rebounds', 'steals']
        for stat in double_test:
            if row[stat] >= 10:
                temp += 1
        if temp > 0:
            dbl_trpl_double = temp
        return {'dbl_trpl_double': dbl_trpl_double,
                'field_goal_percentage': field_goal_percentage,
                'free_throw_percentage': free_throw_percentage,
                'three_point_percentage': three_point_percentage,
                'assist_to_turnover': assist_to_turnover}

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
