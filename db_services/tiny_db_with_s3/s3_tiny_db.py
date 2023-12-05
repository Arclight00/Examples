from tinydb import TinyDB
from tinydbstorage.schema import S3ConfigSchema
from tinydbstorage.storage import S3Storage

from rplus_utils.common.config import config


class TinyDBStorage:
    def __init__(
        self,
        db_file_path: str,
        aws_bucket_name: str = config.s3_bucket_name,
        aws_access_key_id: str = config.aws_access_key_id,
        aws_secret_access_key: str = config.aws_secret_access_key,
        aws_region_name: str = config.region_name,
    ):
        self.conf = S3ConfigSchema.parse_obj(
            {
                "aws_bucket_name": aws_bucket_name,
                "aws_file_path": db_file_path,
                "aws_region_name": aws_region_name,
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key,
            }
        )

        self.db = TinyDB(storage=S3Storage, config=self.conf)

    @classmethod
    def new_ubd_tinydb(cls):
        return cls("ubd/ubd.json").db
