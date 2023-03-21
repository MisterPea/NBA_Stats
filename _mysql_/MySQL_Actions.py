from typing import List, Tuple, Dict
from errno import errorcode
import pandas as pd
import mysql.connector


class MySqlActions:
    """
    Methods for MySQL Interaction:
    Within the confines of these actions, we can chain methods together if need-be.
    e.g. = `MA.import_csv(FILENAME, CSV_MAPPING, TABLE_NAME).close_connection()`
    This is facilitated by the returning of `self` at the conclusion of the method.
    Doubly, we able to do this because these are all essentially side-effects that
    we're running; that is, we're rarely ever needing to return a value to be used elsewhere.
    """

    def __init__(self):
        self.sql_pool = None
        self.create_connection_pool()

    # MAIN CONNECTION ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
    def create_connection_pool(self):
        """Method to create a connection pool"""
        config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'misterpea',
            'database': 'player_stats',
            'password': 'uz1G71fYt5mby3ZOqZ8THAK06',
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

    # DATABASE METHODS ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
    def get_database_list(self):
        """Method that returns the databases in sql instance"""
        try:
            connection = self.sql_pool.get_connection()
            cursor = connection.cursor()
            cursor.execute('SHOW DATABASES')
            for _db in cursor:
                print(_db)
            return self
        except mysql.connector.Error as err:
            print(err)
            return err

    # TABLE METHODS –––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
    def get_table_list(self):
        """Method to that prints all tables in database"""
        try:
            connection = self.sql_pool.get_connection()
            cursor = connection.cursor()
            cursor.execute('SHOW TABLES')
            for _table in cursor:
                print("\033[0;94m" + str(_table) + "\033[0m")
            return self
        except mysql.connector.Error as err:
            print(err)
            return err

    def delete_table(self, table_name: str):
        """Method to delete a table"""
        while True:
            print(
                "\033[0;91m" + f"Are you sure you want to delete table: {table_name}? (y/n): " + "\033[0m", end="")
            user_confirm = input().lower()
            if user_confirm.lower() == 'y':
                try:
                    connection = self.sql_pool.get_connection()
                    cursor = connection.cursor()
                    cursor.execute(f'DROP TABLE {table_name}')
                    print(f'TABLE {table_name} HAS BEEN DELETED')
                    return self
                except mysql.connector.Error as err:
                    print(err)
                    return self
            elif user_confirm.lower() == 'n':
                return self
            else:
                print("Invalid input. Please enter 'yes' or 'no'.")

    def create_tables(self, table_name: str, table_array: List[Tuple[str, str]]) -> any:
        """Method to create table(s).
        This method takes a List of Tuples, where the first value of
        the Tuple is the field name and the second value is the value type
        [('player_slug', 'VARCHAR(50)'),('birthdate', 'DATE'),...]
        """
        try:
            connection = self.sql_pool.get_connection()
            cursor = connection.cursor()
            length = len(table_array) - 1
            compiled_columns = ''
            for i, (name, command) in enumerate(table_array):
                compiled_columns += name + ' ' + command
                if i < length:
                    compiled_columns += ', '
            to_execute = 'CREATE TABLE IF NOT EXISTS ' + table_name + \
                ' (' + compiled_columns + ')'
            cursor.execute(to_execute)
            print(f'TABLET {table_name} CREATED')
            return self
        except mysql.connector.Error as err:
            print(err)
            return self

    def show_table_columns(self, table_name: str):
        """Method to show the column headings"""
        try:
            connection = self.sql_pool.get_connection()
            cursor = connection.cursor()
            cursor.execute(f'SELECT * FROM {table_name} LIMIT 0')
            column_names = [i[0] for i in cursor.description]
            print("\033[1;92m" + ", ".join(column_names) + "\033[0m", end="")
            return self
        except mysql.connector.Error as err:
            print(err)
            return self

    # CSV IMPORT –––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
    def import_csv(self, csv_file: str, mapping: Dict, table_name: str):
        """
        Method to import csv data into a database
        The mapping is a Dict of {'<local_sql_column>': '<source_csv_column'}
        """
        try:
            document = pd.read_csv(csv_file)
            connection = self.sql_pool.get_connection()
            cursor = connection.cursor()
            for _, row in document.iterrows():
                columns = "(" + ','.join(list(mapping)) + ")"
                values = tuple(row[mapping[col]] for col in mapping)
                query = f'INSERT INTO {table_name} {columns} VALUES {values}'
                print(query)
                cursor.execute(query)
            connection.commit()
            print(
                "\033[0;92m" + f'HUZZAH! - Transfer of .csv data to table {table_name} is complete.' + "\033[0m")
            return self
        except mysql.connector.Error as err:
            print("\033[0;91m", err, "\033[0m")
            return self

    # CLOSE METHOD ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
    def close_connection(self):
        """Method to close database connection"""
        connection = self.sql_pool.get_connection()
        connection.close()
        print("\n" + "\033[1;92m" +
              "CONNECTION CLOSED" + "\033[0m")
        return self


CSV_MAPPING_PLAYERS = {'id': 'id', 'full_name': 'full_name',
    'first_name': 'first_name', 'last_name': 'last_name', 'is_active': 'is_active'}

# CSV_MAPPING = {'player_id': 'person_id', 'first_name': 'first_name', 'last_name': 'last_name',
#                'display_first_last': 'display_first_last',
#                'display_last_comma_first': 'display_last_comma_first',
#                'display_fi_last': 'display_fi_last', 'player_slug': 'player_slug',
#                'birthdate': 'birthdate', 'school': 'school', 'country': 'country',
#                'height': 'height', 'weight': 'weight', 'seasons_experience': 'season_exp',
#                'jersey': 'jersey', 'position': 'position', 'roster_status': 'rosterstatus',
#                'team_id': 'team_id', 'team_name': 'team_name',
#                'team_abbreviation': 'team_abbreviation', 'team_code': 'team_code',
#                'team_city': 'team_city', 'from_year': 'from_year', 'to_year': 'to_year',
#                'is_d_league_flag': 'dleague_flag', 'is_nba_flag': 'nba_flag',
#                'games_played_flag': 'games_played_flag', 'draft_year': 'draft_year',
#                'draft_round': 'draft_round', 'draft_number': 'draft_number',
#                'greatest_75_flag': 'greatest_75_flag'}

NIGHTLY_TABLE_MAP = [('id', 'INT AUTO_INCREMENT PRIMARY KEY'), ('date', 'DATE'), ('team', 'TINYTEXT'), ('opponent', 'TINYTEXT'),
                     ('player_name', 'VARCHAR(30)'), ('position', 'VARCHAR(10)'),
                     ('games_played', 'TINYINT'), ('minutes_played', 'TINYINT'),
                     ('field_goals', 'TINYINT'),
                     ('field_goal_attempts', 'TINYINT'),
                     ('three_point_field_goals', 'TINYINT'),
                     ('three_point_field_goal_attempts', 'TINYINT'),
                     ('free_throws', 'TINYINT'),
                     ('free_throw_attempts', 'TINYINT'),
                     ('offensive_rebounds', 'TINYINT'),
                     ('defensive_rebounds', 'TINYINT'), ('total_rebounds', 'TINYINT'),
                     ('assists', 'TINYINT'), ('personal_fouls', 'TINYINT'),
                     ('disqualifications', 'TINYINT'),
                     ('steals', 'TINYINT'), ('turnovers', 'TINYINT'), ('blocks', 'TINYINT'), ('points_scored', 'TINYINT')]

link = '/Users/mothership/Downloads/player_v2.csv'
MA = MySqlActions()
MA.import_csv(link, CSV_MAPPING_PLAYERS, 'active_players').close_connection()
# MA.get_table_list().close_connection()
# MA.create_tables('nightly_player_totals', NIGHTLY_TABLE_MAP).close_connection()
