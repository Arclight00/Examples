from typing import ClassVar

from rplus_graphdb.graph import GraphDb, GraphDbConnection

from rplus_utils.common.config import config


class AwsANGraphDb:
    def __init__(self, connection_class: GraphDbConnection):
        self.graph = GraphDb.from_connection(connection_class)

    @classmethod
    def new_connection_config(cls) -> "ClassVar":
        """Create new connection from configs
        :return: current object class
        """

        return cls(
            GraphDbConnection.from_uri(
                config.graph_conn_uri_writer,
                config.graph_conn_uri_writer,
                pool_size=5,
                max_workers=5,
            )
        )

    @classmethod
    def from_connection_uri(
        cls,
        connection_uri_writer: str = config.graph_conn_uri_writer,
        connection_uri_reader: str = config.graph_conn_uri_reader,
        pool_size: int = 0,
        max_workers: int = 0,
    ) -> "ClassVar":
        """Create new object based on connection uri
        :param connection_uri_writer: string from connection uri writers
        :param connection_uri_reader: string from connection uri readers
        :param pool_size: int pool size
        :param max_workers: int for max workers
        :return: object class
        """

        return cls(
            GraphDbConnection.from_uri(
                connection_uri_writer, connection_uri_reader, pool_size, max_workers
            )
        )

    @classmethod
    def from_connection_class(cls, connection_class: GraphDbConnection) -> "ClassVar":
        """Define new class based on object connection
        :param connection_class: object connection class
        :return: object class
        """
        return cls(connection_class)

    @classmethod
    def new_connection(cls):
        ctl = cls.new_connection_config()
        return ctl.graph
