"""Tests for API key creation"""
import unittest
import mongomock
from key_creation.Api_Key_Management import ApiKeyManagement


class TestApiKeyManagement(unittest.TestCase):
    """Tests for API Key Management"""

    def setUp(self):
        mock_collection = mongomock.MongoClient().db.collection
        objects = [
            {'email': 'abc@abc.com', 'api-key': 'a1b2c3'},
            {'email': 'xyz@abc.com', 'api-key': 'abcd1234'}]
        for obj in objects:
            obj['_id'] = mock_collection.insert_one(obj).inserted_id

        # We pass in the mock collection to top level of the class
        self.mock_api_key_mgmt = ApiKeyManagement(mock_collection)

    def test_generate_key(self):
        """Test for key generator"""
        key = self.mock_api_key_mgmt.generate_key()
        self.assertIsNotNone(key)
        self.assertTrue(isinstance(key, str))

    def test_create_new_key_record(self):
        """Test for record creator"""
        self.mock_api_key_mgmt.email = 'mr_p@abc.com'
        result = self.mock_api_key_mgmt.create_new_key_record()
        self.assertTrue(result['email'] == 'mr_p@abc.com')

    def test_is_existing_email(self):
        """Test for existing email address"""
        self.mock_api_key_mgmt.email = 'abc@abc.com'
        existing_result = self.mock_api_key_mgmt.is_new_email_key()
        self.assertFalse(existing_result)

    def test_is_new_email(self):
        """Test for novel email address"""
        self.mock_api_key_mgmt.email = 'abcd@abc.com'
        existing_result = self.mock_api_key_mgmt.is_new_email_key()
        self.assertTrue(existing_result)

    def test_create_new_key_success(self):
        """Test entire route of key creation-success"""
        result = self.mock_api_key_mgmt.get_api_key('zxy@xyz.com')
        self.assertTrue(result['email'] == 'zxy@xyz.com')

    def test_create_new_key_fail(self):
        """Test entire route of key creation-failure"""
        result = self.mock_api_key_mgmt.get_api_key('abc@abc.com')
        self.assertTrue(result['email'] is None)

    def test_is_a_valid_key(self):
        """Tests if is valid key"""
        result = self.mock_api_key_mgmt.is_valid_api_key('a1b2c3')
        self.assertTrue(result)

    def test_is_not_valid_key(self):
        """Tests if key in not valid"""
        result = self.mock_api_key_mgmt.is_valid_api_key('zzzzzz')
        self.assertFalse(result)
