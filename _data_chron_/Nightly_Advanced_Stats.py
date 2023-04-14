import os
from typing import List
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')


class NightlyAdvancedStats:
    """ Methods to perform calculations for advanced stats """

    def __init__(self) -> None:
        self.sql_pool = None
        self.create_connection_pool()
        self.queries = [
            {'filename': './queries/def_rating_01.sql',
             'columns': ['id', 'date', 'team', 'opponent', 'player_name', 'stops', 'stops_pct', 'team_def_rating', 'def_rating'],
             'table':'adv_stats_player',
             'insert_update':"INSERT"
             },
            {'filename': './queries/off_rating_01.sql',
             'columns': ['game_score', 'usage_rate', 'eff_fg_pct', 'off_rating'],
             'table':'adv_stats_player',
             'insert_update':'UPDATE'
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
                  "CONNECTION POOL CREATED - Advanced Stats:", self.sql_pool, "\033[0m")
        except mysql.connector.Error as err:
            if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
                print('Something is wrong with your username or password')
            elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
                print(f'Database {config["database"]} does not exist')
            else:
                print(err)

    def execute_insert_sql_script(self, filename: str, columns: List[str], table: str):
        """ Method to open and execute a .sql file """
        connection = self.sql_pool.get_connection()
        with open(filename, 'r', encoding='utf-8') as file:
            sql_file = file.read()
            sql_commands = sql_file.split(';')
            for command in sql_commands:
                cursor = connection.cursor()
                cursor.execute(command)
                output = cursor.fetchall()
                keys = ', '.join(columns)
                if len(output) > 0:
                    for row in output:
                        cursor = connection.cursor()
                        query = f"INSERT IGNORE INTO {table} ({keys}) VALUES {row}"
                        cursor.execute(query)
            connection.commit()
        return self

    def execute_update_sql_script(self, filename: str, columns: List[str], table: str):
        """ Method to open and execute a .sql file """

        connection = self.sql_pool.get_connection()
        cursor = connection.cursor()
        with open(filename, 'r', encoding='utf-8') as file:
            sql_file = file.read()
            sql_commands = sql_file.split(';')
            for command in sql_commands:
                cursor.execute(command)
                output = cursor.fetchall()
                if len(output) > 0:
                    for row in output:
                        query_inner = ', '.join([f'{k}={v}' for k, v in zip(columns, row[1:])])
                        query = f'UPDATE {table} SET {query_inner} WHERE id={row[0]}'
                        cursor.execute(query)
            connection.commit()
        return self

    def add_stats(self):
        """ Method to cycle through and execute info within query dict """
        for _q in self.queries:
            filename = _q['filename']
            columns = _q['columns']
            table = _q['table']
            insert_update = _q['insert_update']
            if insert_update == 'INSERT':
                self.execute_insert_sql_script(filename, columns, table)
            else:
                self.execute_update_sql_script(filename, columns, table)

    def close_connection(self):
        """Method to close database connection"""
        connection = self.sql_pool.get_connection()
        connection.close()
        print("\n" + "\033[1;92m" +
              "CONNECTION CLOSED" + "\033[0m")
        return self
