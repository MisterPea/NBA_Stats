"""Methods to create api key and write to database"""
import random
import string
from typing import Dict
from pymongo import MongoClient


class DatabaseWriteError(Exception):
    "Raised on write errors to mongo database"


class ApiKeyManagement:
    """Class to handle the creation of API KEYS"""

    def __init__(self, collection=None) -> None:
        client = MongoClient('mongodb://root:rootpassword@localhost:27017')
        database = client['client']
        self.collection = collection or database['api-data']
        self.email = None

    def get_api_key(self, email: str):
        """Root method to assign a api-key to an email, if that email is unique"""
        self.email = email
        if self.is_new_email_key():
            return self.create_new_key_record()
        return {
            'success': 'email already exists',
            'key': None,
            'email': None
        }

    def generate_key(self) -> str:
        """Method to generate a random key between 30 and 40 characters"""
        min_length = 30
        max_length = 40
        characters = string.ascii_letters + string.digits
        random_length = random.randint(min_length, max_length)
        random_string = ''.join(
            random.choice(characters) for _ in range(random_length))
        return random_string

    def is_new_email_key(self) -> bool:
        """
        Method to determine if email is new.
        Returns True if email is new else False
        """
        is_entry = self.collection.find_one({'email': self.email})
        return is_entry is None

    def is_valid_api_key(self, test_key: str) -> bool:
        """Method that compares test_key argument against database"""
        is_valid_key = self.collection.find_one({'api-key': test_key})
        return is_valid_key is not None

    def create_new_key_record(self) -> Dict:
        """
        Method to write key and email to database.
        Upon successful write, the key is returned. If the write fails 
        a message will indicate as such
        """
        key = self.generate_key()
        data = {'email': self.email, 'api-key': key}
        try:
            result = self.collection.insert_one(data)
            return {
                'success': f'created {result}',
                'key': key,
                'email': self.email
            }
        except DatabaseWriteError as _e:
            print('An exception occurred in `create_new_key_record()`::', _e)
            return {}
