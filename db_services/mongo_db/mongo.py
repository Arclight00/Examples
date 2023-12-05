from typing import List, Any, Dict, Union, Tuple

import pymongo
from pymongo import UpdateOne, UpdateMany, InsertOne
from pymongo.errors import ExecutionTimeout

from rplus_utils.common.config import config
from rplus_utils.logger.logger import Logging
from rplus_utils.utils.measureit import measure_it

MONGO_TIMEOUT_MS = config.mongo_timeout_ms
MONGO_MIN_POOL_SIZE = config.mongo_min_pool_size
MONGO_MAX_POOL_SIZE = config.mongo_max_pool_size
MONGO_RETRY_WRITES = config.mongo_retry_write
MONGO_MESSAGE_TIMEOUT = config.mongo_message_timeout
log_base = "Records count effected = {}"


class MongoDatabase:
    def __init__(self, db_url: str, database_name: str):
        self.__cl = pymongo.MongoClient(
            db_url,
            serverSelectionTimeoutMS=MONGO_TIMEOUT_MS,
            minPoolSize=MONGO_MIN_POOL_SIZE,
            maxPoolSize=MONGO_MAX_POOL_SIZE,
            retryReads=MONGO_RETRY_WRITES,
        )
        self.__db = self.__cl[database_name]
        self._message_log = "Records fetched with length = {}"

    def get_connection(self) -> pymongo.MongoClient:
        """Get object connection only to used in other place
        :return:
        """
        return self.__cl

    def count(self, collection_name: str, query: Dict[str, Any]) -> int:
        """Create indexing on specified collection, if index is exists mongo will ignore it
        :param collection_name: collection name that need to index
        :param query: filter data for specified condition
        :return: integer count data
        """
        try:
            return self.__db[collection_name].count_documents(query)
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
            return ""

    def bulk_write(
        self,
        collection_name: str,
        docs: Union[List[UpdateOne], List[UpdateMany], List[InsertOne]],
    ) -> None:
        """Bulk operation to database
        :param collection_name: string collection name
        :param docs: list of method mongodb
        :return: None
        """
        try:
            return self.__db[collection_name].bulk_write(docs)
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
            return None

    def create_index(
        self,
        collection_name: str,
        index_key: List[Tuple[str, int]],
        is_unique: bool = False,
    ) -> str:
        """Create indexing on specified collection, if index is exists mongo will ignore it, it will running on background
        :param collection_name: collection name that need to index
        :param index_key: is payload to indexing: eg: [("user_id", 1), ("datetime", -1)]
        :param is_unique: is current indexing need unique keys, default is false
        :return: string indexing name
        """
        try:
            return self.__db[collection_name].ensure_index(
                index_key, unique=is_unique, background=True
            )
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
            return ""

    def insert(self, collection_name: str, data: Dict[str, Any]) -> str:
        """Inserting single data to our database
        :param collection_name: string collection name
        :param data: dictionary data
        :return: string record id
        """
        try:
            record_id = self.__db[collection_name].insert(data)
            Logging.info("Record inserted - id: {}".format(record_id))
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
            return ""

    @measure_it
    def find(
        self,
        collection_name: str,
        query_param: Dict[str, Any],
        select_fields: Dict[str, Any] = {},
    ) -> Dict[str, Any]:
        """Find single data from our database for specified filter
        :param collection_name: string collection name
        :param query_param: dictionary data for filtering data in our database
        :return: Dictionary object mongodb
        """
        try:
            if len(select_fields) > 0:
                select_fields["_id"] = 0
            record_id = self.__db[collection_name].find_one(query_param, select_fields)
            Logging.info("Record fetched - id: {}".format(record_id))
            return record_id
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
            return dict()

    @measure_it
    def find_without_cache(
        self,
        collection_name: str,
        query_param: Dict[str, Any],
        select_fields: Dict[str, Any] = {},
    ) -> Dict[str, Any]:
        """Find single data from our database for specified filter
        :param collection_name: string collection name
        :param query_param: dictionary data for filtering data in our database
        :return: Dictionary object mongodb
        """
        try:
            if len(select_fields) > 0:
                select_fields["_id"] = 0
            record_id = self.__db[collection_name].find_one(query_param, select_fields)
            Logging.info("Record fetched - id: {}".format(record_id))
            return record_id
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
            return dict()

    @measure_it
    def find_and_sorted_value(
        self,
        collection_name: str,
        query: Dict[str, Any],
        field_to_be_sorted_on: str,
        is_asc: bool = False,
        page_no: int = None,
        page_size: int = None,
        select_fields: Dict[str, Any] = {},
    ) -> List[Dict[str, Any]]:
        """Find data in our database and sorted value based on key selected
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :param field_to_be_sorted_on: string key data that need to sort on
        :param is_asc: is data sorted asc or desc ?
        :param page_no: what is current page
        :param page_size: how much content per per page
        :return: List of sorted data based on specified key
        """
        try:
            if len(select_fields) > 0:
                select_fields["_id"] = 0
            if page_no is None or page_size is None:
                cursor = (
                    self.__db[collection_name]
                    .find(query, select_fields)
                    .sort(
                        [
                            (
                                "score",
                                pymongo.DESCENDING
                                if is_asc is False
                                else pymongo.ASCENDING,
                            ),
                            (
                                field_to_be_sorted_on,
                                pymongo.DESCENDING
                                if is_asc is False
                                else pymongo.ASCENDING,
                            ),
                        ]
                    )
                )
                result = [i for i in cursor]
                Logging.info(self._message_log.format(len(result)))
                return result

            if page_no in [0, 1]:
                cursor = (
                    self.__db[collection_name]
                    .find(query, select_fields)
                    .sort(
                        [
                            (
                                "score",
                                pymongo.DESCENDING
                                if is_asc is False
                                else pymongo.ASCENDING,
                            ),
                            (
                                field_to_be_sorted_on,
                                pymongo.DESCENDING
                                if is_asc is False
                                else pymongo.ASCENDING,
                            ),
                        ]
                    )
                    .limit(page_size or 10)
                )
                result = [i for i in cursor]
                Logging.info(self._message_log.format(len(result)))
                return result

            cursor = (
                self.__db[collection_name]
                .find(query, select_fields)
                .sort(
                    [
                        (
                            "score",
                            pymongo.DESCENDING
                            if is_asc is False
                            else pymongo.ASCENDING,
                        ),
                        (
                            field_to_be_sorted_on,
                            pymongo.DESCENDING
                            if is_asc is False
                            else pymongo.ASCENDING,
                        ),
                    ]
                )
                .skip((page_no - 1) * page_size)
                .limit(page_size)
            )
            result = [i for i in cursor]
            Logging.info(self._message_log.format(len(result)))
            return result
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
            return []

    @measure_it
    def find_and_sorted_recommendations(
        self,
        collection_name: str,
        query: Dict[str, Any],
        select_fields: Dict[str, Any] = {},
    ) -> List[Dict[str, Any]]:
        try:
            if len(select_fields) > 0:
                select_fields["_id"] = 0
            cursor = self.__db[collection_name].find(query, select_fields)
            result = [i for i in cursor]
            Logging.info(self._message_log.format(len(result)))
            return result

        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
            return []

    @measure_it
    def find_and_sorted_recommendations_without_cache(
        self,
        collection_name: str,
        query: Dict[str, Any],
        select_fields: Dict[str, Any] = {},
    ) -> List[Dict[str, Any]]:
        try:
            if len(select_fields) > 0:
                select_fields["_id"] = 0
            cursor = self.__db[collection_name].find(query, select_fields)
            result = [i for i in cursor]
            Logging.info(self._message_log.format(len(result)))
            return result

        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
            return []

    @measure_it
    def skip_and_limit_sorted_value(
        self,
        collection_name: str,
        query: Dict[str, Any],
        field_to_be_sorted_on: str,
        limit_count: int,
        is_asc: bool = False,
        skip_count: int = 0,
        select_fields: Dict[str, Any] = {},
    ) -> List[Dict[str, Any]]:
        """Find data in our database and sorted value based on key selected
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :param field_to_be_sorted_on: string key data that need to sort on
        :param is_asc: is data sorted asc or desc ?
        :param limit_count: limit for the records projection
        :param skip_count: how many content need to skip
        :return: List of sorted data based on specified key
        """
        try:
            if len(select_fields) > 0:
                select_fields["_id"] = 0
            cursor = (
                self.__db[collection_name]
                .find(query, select_fields)
                .sort(
                    [
                        (
                            "score",
                            pymongo.DESCENDING
                            if is_asc is False
                            else pymongo.ASCENDING,
                        ),
                        (
                            field_to_be_sorted_on,
                            pymongo.DESCENDING
                            if is_asc is False
                            else pymongo.ASCENDING,
                        ),
                    ]
                )
                .skip(skip_count)
                .limit(limit_count)
            )
            result = [i for i in cursor]
            Logging.info(self._message_log.format(len(result)))
            return result
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
            return []

    @measure_it
    def find_aggregated(
        self, collection_name: str, query: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Query data with aggregate function in our database
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :return: list of data based on our query
        """
        try:
            result = [i for i in self.__db[collection_name].aggregate(query)]
            Logging.info(self._message_log.format(len(result)))
            return result
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
            return []

    @measure_it
    def aggregated_result(
            self,
            collection_name: str,
            match: List[Dict[str, Any]],
            group: List[Dict[str, Any]],
            limit: int = 1000000
    ) -> List[Dict[str, Any]]:
        """Query data with aggregate function in our database
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :param group: dictionary data for aggregating results
        :param limit: no of records to be fetched
        :return: list of data based on our query
        """
        try:

            result = [i for i in self.__db[collection_name].aggregate([
                {'$match': match},
                {'$group': group},
                {'$limit': limit}
            ])]
            Logging.info(f"return result with limit: {limit}")
            Logging.info(self._message_log.format(len(result)))
            return result
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
            return []

    def update(
        self,
        collection_name: str,
        query: Dict[str, Any],
        data: Union[List[Dict[str, Any]], Dict[str, Any]],
    ):
        """Update single record to our database based on specified filter in query
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :param data: it can be list of dictionary or only single dictinary
        :return: None
        """
        try:
            cursor = self.__db[collection_name].update_one(query, data, upsert=True)
            Logging.info(log_base.format(cursor.matched_count))
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)

    def delete(self, collection_name: str, query: Dict[str, Any]):
        """Delete data from our database for specified filter
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :return: None
        """
        try:
            cursor = self.__db[collection_name].delete_one(query)
            Logging.info(log_base.format(cursor.deleted_count))
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)

    def distinct(self, field: str, collection_name: str, query: Dict[str, Any]):
        try:
            cursor = self.__db[collection_name].distinct(field, query)
            # Logging.info(log_base.format(cursor.matched_count))
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
        return cursor

    def update_many(
        self,
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
        try:
            cursor = self.__db[collection_name].update_many(query, data)
            Logging.info(log_base.format(cursor.matched_count))
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)

    @measure_it
    def find_many(
        self,
        collection_name: str,
        query: Dict[str, Any],
        select_fields: Dict[str, Any] = {},
    ) -> List[Dict[str, Any]]:
        """Find data from our database for specified filter
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :return: List of dictionary
        """
        try:
            if len(select_fields) > 0:
                select_fields["_id"] = 0
            result = [i for i in self.__db[collection_name].find(query, select_fields)]
            Logging.info(self._message_log.format(len(result)))
            return result
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
            return []

    @measure_it
    def find_many_without_cache(
        self,
        collection_name: str,
        query: Dict[str, Any],
        select_fields: Dict[str, Any] = {},
    ) -> List[Dict[str, Any]]:
        """Find data from our database for specified filter
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :return: List of dictionary
        """
        try:
            if len(select_fields) > 0:
                select_fields["_id"] = 0
            result = [i for i in self.__db[collection_name].find(query, select_fields)]
            Logging.info(self._message_log.format(len(result)))
            return result
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
            return []

    def insert_many(self, collection_name: str, data: List[Dict[str, Any]]):
        """Insert bulking data to our database
        :param data: list of dictionary data
        :param collection_name: string collection name
        :return: None
        """
        try:
            self.__db[collection_name].insert_many(data, ordered=False)
            Logging.info("All the Data has been Exported to Mongo DB Server .... ")
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)

    def delete_many(self, collection_name: str, query: Dict[str, Any]):
        """Delete many data from our database for specified filter
        :param collection_name: string collection name
        :param query: dictionary data for filtering data in our database
        :return: None
        """
        try:
            cursor = self.__db[collection_name].delete_many(query)
            Logging.info("Records effected = {}".format(cursor.deleted_count))
        except ExecutionTimeout:
            Logging.info(MONGO_MESSAGE_TIMEOUT)
