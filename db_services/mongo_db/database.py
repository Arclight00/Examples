from typing import List, Dict, Any, Union, Tuple
from urllib.parse import quote_plus

from pymongo import UpdateOne, UpdateMany, InsertOne, MongoClient

from rplus_utils.common.config import config
from rplus_utils.db_services.mongo_db.mongo import MongoDatabase as MongoConnection


class MongoDbConnection:
    cl: MongoConnection

    def __init__(self, uri, db_name):
        MongoDbConnection.cl = MongoConnection(uri, db_name)

    @staticmethod
    def get_connection() -> MongoClient:
        """Get connection object from mongoDB
        :return: mongo Connection Object
        """
        return MongoDbConnection.cl.get_connection()

    @staticmethod
    def count(collection_name: str, query: Dict[str, Any]) -> int:
        """Create indexing on specified collection, if index is exists mongo will ignore it
        :param collection_name: collection name that need to index
        :param query: is filter data based on selected filter
        :return: integer count data
        """
        return MongoDbConnection.cl.count(collection_name, query)

    @staticmethod
    def create_index(
        collection_name: str, index_key: List[Tuple[str, int]], is_unique: bool
    ) -> str:
        """Create indexing on specified collection, if index is exists mongo will ignore it
        :param collection_name: collection name that need to index
        :param index_key: is payload to indexing: eg: [("user_id", 1), ("datetime", -1)]
        :param is_unique: is indexing is unique type
        :return: string index name
        """
        return MongoDbConnection.cl.create_index(collection_name, index_key, is_unique)

    @staticmethod
    def bulk_write(
        collection_name: str,
        docs: Union[List[UpdateOne], List[UpdateMany], List[InsertOne]],
    ) -> None:
        """Bulk operation to insert or update value multiple
        :param collection_name: string collection name
        :param docs: list of method mongodb
        :return: none
        """
        return MongoDbConnection.cl.bulk_write(collection_name, docs)

    @staticmethod
    def insert_many(collection_name: str, data: List[Dict[str, Any]]):
        """Insert bulking data to our database
        :param data: list of dictionary data
        :param collection_name: string collection name
        :return: None
        """
        MongoDbConnection.cl.insert_many(collection_name, data)

    @staticmethod
    def insert(collection_name: str, data: Dict[str, Any]):
        """Inserting data to our database
        :param collection_name: string collection name
        :param data: dictionary data
        :return: None
        """
        MongoDbConnection.cl.insert(collection_name, data)

    @staticmethod
    def find_one(
        collection_name: str,
        query_param: Dict[str, Any],
        select_fields: Dict[str, Any] = {},
    ) -> Dict[str, Any]:
        """Find single data from our database for specified filter
        :param collection_name: string collection name
        :param query_param: dictionary data for filtering data in our database
        :return: Dictionary object mongodb
        """
        return MongoDbConnection.cl.find(
            collection_name, query_param, select_fields=select_fields
        )

    @staticmethod
    def find(
        collection_name: str, query: Dict[str, Any], select_fields: Dict[str, Any] = {}
    ) -> List[Dict[str, Any]]:
        """Find data from our database for specified filter
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :return: List of dictionary
        """
        return MongoDbConnection.cl.find_many(
            collection_name, query, select_fields=select_fields
        )

    @staticmethod
    def find_without_cache(
        collection_name: str, query: Dict[str, Any], select_fields: Dict[str, Any] = {}
    ) -> List[Dict[str, Any]]:
        """Find data from our database for specified filter
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :return: List of dictionary
        """
        return MongoDbConnection.cl.find_many_without_cache(
            collection_name, query, select_fields=select_fields
        )

    @staticmethod
    def find_sorted(
        collection_name: str,
        query: Dict[str, Any],
        field_to_be_sorted_on: str,
        is_asc: bool = False,
        select_fields: Dict[str, Any] = {},
    ) -> List[Dict[str, Any]]:
        """Find data in our database and sorted value based on key selected
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :param field_to_be_sorted_on: string key data that need to sort on
        :param is_asc: is data sorted asc or desc ?
        :return: List of sorted data based on specified key
        """
        return MongoDbConnection.cl.find_and_sorted_value(
            collection_name,
            query,
            field_to_be_sorted_on,
            is_asc,
            select_fields=select_fields,
        )

    @staticmethod
    def find_and_sorted_recommendations(
        collection_name: str, query: Dict[str, Any], select_fields: Dict[str, Any] = {}
    ) -> List[Dict[str, Any]]:
        return MongoDbConnection.cl.find_and_sorted_recommendations(
            collection_name, query, select_fields=select_fields
        )

    @staticmethod
    def find_and_sorted_recommendations_without_cache(
        collection_name: str, query: Dict[str, Any], select_fields: Dict[str, Any] = {}
    ) -> List[Dict[str, Any]]:
        return MongoDbConnection.cl.find_and_sorted_recommendations_without_cache(
            collection_name, query, select_fields=select_fields
        )

    @staticmethod
    def find_aggregated_result(
        collection_name: str, query: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Query data with aggregate function in our database
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :return: list of data based on our query
        """
        return MongoDbConnection.cl.find_aggregated(collection_name, query)

    @staticmethod
    def update(
        collection_name: str,
        query: Dict[str, Any],
        data: Union[List[Dict[str, Any]], Dict[str, Any]],
    ):
        """Update record to our database based on specified filter in query
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :param data: it can be list of dictionary or only single dictinary
        :return: None
        """
        MongoDbConnection.cl.update(collection_name, query, data)

    @staticmethod
    def delete(collection_name: str, query: Dict[str, Any]):
        """Delete data from our database for specified filter
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :return: None
        """
        MongoDbConnection.cl.delete(collection_name, query)

    @staticmethod
    def aggregated_result(
        collection_name: str,
        group: List[Dict[str, Any]],
        match: List[Dict[str, Any]]={},
        limit: int = 1000000
    ):
        """Query data with aggregate function in our database
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :param group: dictionary data for aggregating results
        :param limit: no of records to be fetched
        :return: list of data based on our query
        """
        return MongoDbConnection.cl.aggregated_result(collection_name, match, group, limit)


    @staticmethod
    def distinct_records(field: str, collection_name: str, query: Dict[str, Any]):
        MongoDbConnection.cl.distinct(field, collection_name, query)

    @classmethod
    def new_connection_conviva(cls):
        DB_URI = '''mongodb://{user}:{password}@ec2-13-229-77-22.ap-southeast-1.compute.amazonaws.com:27017/conviva?directConnection=true'''.format(
            user=config.conviva_mongodb_user,
            password=quote_plus(config.conviva_mongodb_password),
        )
        return cls(DB_URI, config.conviva_mongodb_ubd_database)
