import ast
import base64
import gzip
import json
from copy import deepcopy
from typing import AnyStr, ByteString, Union, Dict, Any, List, Iterator, Tuple
from urllib.parse import urlparse

import redis

from rplus_utils import Logging
from rplus_utils.common.config import config


class RedisConnection:
    def __init__(
        self,
        host: str,
        port: int,
        db: int,
        username: str,
        password: str,
        con_uri: str,
    ):
        self.host = host
        self.port = port
        self.db = db
        self.username = username
        self.password = password
        self.uri = con_uri
        any_username = len(self.username) > 0
        any_password = len(self.password) > 0
        credential = {
            "host": self.host,
            "port": self.port,
            "db": self.db,
        }
        if all([any_username, any_password]):
            credential["username"] = self.username
            credential["password"] = self.password

        self.pool = redis.ConnectionPool(**credential)
        self.conn = redis.StrictRedis(
            connection_pool=self.pool,
            max_connections=10,
            retry_on_timeout=True,
            socket_timeout=10_0000,
            socket_connect_timeout=10_0000,
        )

    def encode_string(self, text: str) -> str:
        """Base64 encode string from plain text
        :param text: string plain text
        :return: string base 64 value
        """
        return base64.b64encode(text.encode("ascii")).decode("ascii")

    def decode_string(self, text: str) -> str:
        """Base64 encode string from plain text
        :param text: string plain text
        :return: string base 64 value
        """
        return base64.b64decode(text.encode("ascii")).decode("ascii")

    @property
    def get_host(self) -> str:
        """Get current host
        :return: string host
        """
        return self.host

    @property
    def get_port(self) -> int:
        """Get current port redis
        :return: int port
        """
        return self.port

    def set_db(
        self,
        db: int,
    ) -> bool:
        """Set redis db selected
        :param db: int db eg: 0, 1, 2, 3, 4, 5 ...
        :return: none
        """
        self.db = db
        return True

    @property
    def get_db(self) -> int:
        """Get current db selected
        :return: int db selected
        """
        return self.db

    @property
    def get_username(self) -> str:
        """Get current username
        :return: string username
        """
        return self.username

    @property
    def get_password(self) -> str:
        """Get current password
        :return: string password
        """
        return self.password

    @property
    def get_credential(self) -> Dict[str, Any]:
        """Get current credential to connect to redis
        :return: dictionary value
        """
        return {
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "username": self.username,
            "password": self.password,
        }

    def __str__(self):
        return "RedisConnection({}, {}, {}, {}, {})".format(
            self.host,
            self.port,
            self.db,
            self.username,
            self.password,
        )

    @classmethod
    def from_uri(cls, uri: str) -> "RedisConnection":
        """Create object connection class from uri
        this will receive uri like this:
            redis://example:secret@localhost:6379/0
        :param uri: string redis uri
        :return: class object
        """
        p = urlparse(uri)
        return cls(p.hostname, p.port, int(p.path[1:]), p.username, p.password, uri)

    def delete(self, keys: List[str]) -> int:
        """Delete records in redis based on specified keys
        :param keys: string key
        :return: boolean result
        """
        cnt = 0
        for key in keys:
            self.conn.delete(key)
            cnt += 1
        return cnt

    def scan_iter(self, key: str) -> Iterator[Union[Union[str, bytes], Any]]:
        """Scan all redis database
        :param key: string keys
        :return: list of string keys
        """
        return self.conn.scan_iter(key)

    def generate_sequence_number(self, key: str) -> str:
        """Generate sequence number based on specified key
            example:
                1 -> will return as 000001
                2 -> will return as 000002
        :param key: string key name
        :return: string sequence number
        """
        last_sequence = RedisConnection.get(key)
        if last_sequence is None:
            init_number = 1
            self.set(key, init_number, ttl=0)
            return str(init_number).zfill(10)

        self.incr(key)
        seq = RedisConnection.get(key)
        return str(seq).zfill(10)

    def get(self, key: str) -> Union[ByteString, AnyStr, int, float, None, bytes]:
        """Get value from specified key
        :param key: string key
        :return: it can be string bytes string, int or float
        """
        hash_key = self.encode_string(key)
        v = self.conn.get(hash_key)
        # if any value from it then decode to utf 8, cause default data type is bytes
        if v is not None and len(v) > 0:
            return v.decode("utf-8")

        return None

    def keys(
        self,
        key: str,
    ) -> Union[List[Dict[str, Any]], List[str], List[float], List[int]]:
        """Get data from redis for specified keys
        :param key: string key
        :return: boolean (true, false)
        """
        tmp = []
        for k in self.conn.keys(key):
            if k is not None and len(k) > 0:
                tmp.append(json.loads(self.conn.get(k.decode("utf-8")).decode("utf-8")))
        return tmp

    def get_plain_key(
        self,
        key: str,
        compression: bool = True,
        decode: bool = True
    ) -> Union[ByteString, AnyStr, int, float, None]:
        """Get value from specified key
        :param decode: it can be True or False
        :param key: string key
        :param compression: compression method
        :return: it can be string bytes string, int or float
        """
        v = self.conn.get(key)
        # if any value from it then decode to utf 8, cause default data type is bytes
        if v is not None and len(v) > 0:
            if compression:
                return gzip.decompress(v).decode("utf-8")
            if decode:
                return v.decode("utf-8")
            return v
        return None

    def mset_plain_key(
        self,
        data: Dict[str, Any],
        ttl: int = 60 * 10,
        compression: bool = True,
    ) -> bool:
        """Save data to redis using multiple keys
        :param data: dictionary data with already assign key and value
        :param ttl: default value for time to live
        :param compression: compression method to used
        :return: boolean (true, false)
        """

        if compression is True:
            records = dict()
            for k, v in data.items():
                records[k] = gzip.compress(json.dumps(v).encode("utf-8"))

            return self.conn.mset(records)

        if ttl == 0:
            return self.conn.mset(data)

        return self.conn.mset(data)

    def set_plain_key(
        self,
        key: str,
        value: Union[ByteString, AnyStr, int, float],
        ttl: int = 60 * 10,
        compression: bool = True,
    ) -> bool:
        """Save data to redis by specified keys
        :param key: string key
        :param value: it can be bytes string, string, int or float
        :param ttl: default value for time to live
        :param compression: compression method to used
        :return: boolean (true, false)
        """
        if compression is True:
            return self.conn.set(
                key, gzip.compress(value.encode("utf-8")), keepttl=False
            )

        if ttl == 0:
            return self.conn.set(key, value, keepttl=False)

        return self.conn.set(key, value, keepttl=False)

    def set(
        self,
        key: str,
        value: Union[ByteString, AnyStr, int, float],
        ttl: int = 60 * 10,
    ) -> bool:
        """Save data to redis by specified keys
        :param key: string key
        :param value: it can be bytes string, string, int or float
        :param ttl: default value for time to live
        :return: boolean (true, false)
        """
        hash_key = self.encode_string(key)
        if ttl == 0:
            return self.conn.set(hash_key, value, keepttl=False)

        return self.conn.set(hash_key, value, keepttl=False)

    def incr(
        self,
        key: str,
    ) -> int:
        """Increment value in specified keysredis_cache.py
        :param key: string key
        :return: boolean (true, false)
        """
        hash_key = self.encode_string(key)
        return self.conn.incr(hash_key)

    def chunk_array_keys(
        keys: List[str],
        chunk_size: int = 1000,
    ) -> List[List[str]]:
        """Split array of keys into chunks
        :param keys: list of keys
        :param chunk_size: size of chunk
        :return: list of chunks
        """
        return [keys[i : i + chunk_size] for i in range(0, len(keys), chunk_size)]

    def fetch_all_keys(self, master_key) -> List[str]:
        """Fetch all keys only from redis
        :return: list of all keys in redis
        """
        # get all keys
        keys = deepcopy(self.conn.keys(master_key))
        # close connection
        return keys

    def fetch_value_from_keys(
        self,
        keys: List[str],
    ) -> Tuple[List[str], List[Any]]:
        """Fetch value from keys
        :param keys: list of keys
        """
        q = []
        for chunk_keys in RedisConnection.chunk_array_keys(keys):
            n = len(chunk_keys)
            Logging.info(f"fetch value from : {n} keys")
            # setup pipeline
            p = self.conn.pipeline()
            # get value from keys
            try:
                _ = [p.hgetall(key) for key in keys]
                # execute pipeline
                q += [result for result in p.execute()]
            except Exception as e:
                _ = [p.get(key) for key in chunk_keys]
                # execute pipeline
                q += [result.decode("utf-8") for result in p.execute()]
            len_q = len(q)
            # close connection
        q = [i.replace("null", "None") for i in q]
        values = [ast.literal_eval(i) for i in q]
        return keys, values

    def fetch_master_key_values(
        self,
        master_key: List[str],
    ) -> Tuple[List[str], List[Any]]:
        all_keys = self.fetch_all_keys(master_key)
        return self.fetch_value_from_keys(all_keys)

    @classmethod
    def new_rplus_redis_cluster(cls):
        """Please do not create any global variable, put inside class
        use if only needed
        :return:
        """
        return cls(
            host=config.rplus_redis_host,
            port=config.rplus_redis_port,
            db=config.rplus_redis_user_db,
            username=config.rplus_redis_username,
            password=config.rplus_redis_password,
            con_uri="redis://{}:{}@{}:{}/{}".format(
                config.rplus_redis_username,
                config.rplus_redis_password,
                config.rplus_redis_host,
                config.rplus_redis_port,
                config.rplus_redis_user_db,
            ),
        )

    @classmethod
    def new_rplus_redis_content(cls):
        """Please do not create any global variable, put inside class
        use if only needed
        :return:
        """
        return cls(
            host=config.rplus_redis_host,
            port=config.rplus_redis_port,
            db=config.rplus_redis_content_db,
            username=config.rplus_redis_username,
            password=config.rplus_redis_password,
            con_uri="redis://{}:{}@{}:{}/{}".format(
                config.rplus_redis_username,
                config.rplus_redis_password,
                config.rplus_redis_host,
                config.rplus_redis_port,
                config.rplus_redis_content_db,
            ),
        )

    @classmethod
    def new_aiml_redis_reader(cls):
        """Please do not create any global variable, put inside class
        use if only needed
        :return:
        """
        return cls(
            host=config.redis_host,
            port=config.redis_reader_port,
            db=config.redis_default_db,
            username=config.redis_username,
            password=config.redis_password,
            con_uri="redis://{}:{}@{}:{}/{}".format(
                config.redis_username,
                config.redis_password,
                config.redis_host,
                config.redis_reader_port,
                config.redis_default_db,
            ),
        )

    @classmethod
    def new_aiml_redis_writer(cls):
        """Please do not create any global variable, put inside class
        use if only needed
        :return:
        """
        return cls(
            host=config.redis_host,
            port=config.redis_writer_port,
            db=config.redis_default_db,
            username=config.redis_username,
            password=config.redis_password,
            con_uri="redis://{}:{}@{}:{}/{}".format(
                config.redis_username,
                config.redis_password,
                config.redis_host,
                config.redis_writer_port,
                config.redis_default_db,
            ),
        )

    @classmethod
    def new_rce_redis_cluster(cls):
        """Please do not create any global variable, put inside class
        use if only needed
        :return:
        """
        return cls(
            host=config.redis_host,
            port=config.redis_reader_port,
            db=config.redis_recommendation_db,
            username=config.redis_username,
            password=config.redis_password,
            con_uri="redis://{}:{}@{}:{}/{}".format(
                config.redis_username,
                config.redis_password,
                config.redis_host,
                config.redis_reader_port,
                config.redis_recommendation_db,
            ),
        )

    @classmethod
    def new_rce_redis_writer(cls):
        """Please do not create any global variable, put inside class
        use if only needed
        :return:
        """
        return cls(
            host=config.redis_host,
            port=config.redis_writer_port,
            db=config.redis_recommendation_db,
            username=config.redis_username,
            password=config.redis_password,
            con_uri="redis://{}:{}@{}:{}/{}".format(
                config.redis_username,
                config.redis_password,
                config.redis_host,
                config.redis_writer_port,
                config.redis_recommendation_db,
            ),
        )
