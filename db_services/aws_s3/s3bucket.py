import gzip
import pickle
from io import StringIO, BytesIO

import boto3
from pandas import read_csv, DataFrame

from rplus_utils.common.config import config
from rplus_utils.logger import Logging


class S3Services:
    def __init__(
        self,
        boto_object: boto3.resource,
        bucket_name: str = config.s3_bucket_name,
    ):
        self.resource = boto_object
        self.bucket_name = bucket_name

    @classmethod
    def from_connection(
        cls,
        access_key: str = config.aws_access_key_id,
        secret_key: str = config.aws_secret_access_key,
        region_name: str = config.region_name,
    ) -> "S3Services":
        try:
            conn = boto3.resource(
                "s3",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region_name,
            )
        except Exception as e:
            raise "boto3 resource object creation failed {}".format(str(e))
        return cls(conn)

    def read_csv_from_s3(
        self, object_name=None, bucket_name: str = config.s3_bucket_name
    ) -> DataFrame:
        """
        This function returns dataframe object of csv file stored in S3
        :param bucket_name: s3 bucket name
        :param object_name: Path of the object in S3
        :return: dataframe object pandas
        """
        try:
            content_object = self.resource.Object(bucket_name, object_name)
            csv_string = content_object.get()["Body"].read().decode("utf-8")
            df = read_csv(StringIO(csv_string))
            Logging.info("File {} has been read successfully".format(object_name))
            return df

        except Exception as e:
            raise "Unable to find file no such file exists {}".format(str(e))

    def write_csv_to_s3(
        self,
        object_name=None,
        df_to_upload=None,
        bucket_name: str = config.s3_bucket_name,
    ) -> None:
        """
        Function to write csv in S3
        :param bucket_name: Name of the bucket where csv shall be stored
        :param object_name: Path of the object in S3
        :param df_to_upload: dataframe to be stored as csv
        :return:
        """
        try:
            csv_buffer = StringIO()
            df_to_upload.to_csv(csv_buffer, header=True, index=False)
            content_object = self.resource.Object(bucket_name, object_name)
            content_object.put(Body=csv_buffer.getvalue())
            Logging.info("Successfully dump data into s3")
        except Exception as e:
            raise ("Error while dumping into s3 for the object {}".format(str(e)))

    def write_pickles_to_s3(
        self, object_name=None, data=None, bucket_name: str = config.s3_bucket_name
    ) -> None:
        try:
            Logging.info(f"Start dumping {bucket_name}/{object_name}  into s3")
            pickle_buffer = BytesIO()
            data.to_pickle(pickle_buffer, compression="gzip")
            self.resource.Object(bucket_name, object_name).put(
                Body=pickle_buffer.getvalue()
            )
            Logging.info(
                f"Successfully dumped {bucket_name}/{object_name} data into s3"
            )
        except Exception as e:
            Logging.error(
                f"Error while dumping {bucket_name}/{object_name} to S3, Exception: {e}"
            )

    def read_pickles_from_s3(
        self, object_name=None, bucket_name: str = config.s3_bucket_name
    ):
        try:
            Logging.info(f"Start reading {bucket_name}/{object_name} file from s3")
            content_object = self.resource.Object(bucket_name, object_name)
            read_file = content_object.get()["Body"].read()
            zipfile = BytesIO(read_file)
            with gzip.GzipFile(fileobj=zipfile) as gzipfile:
                content = gzipfile.read()
            loaded_pickle = pickle.loads(content)
            Logging.info(f"File {bucket_name}/{object_name} has been read successfully")
            return loaded_pickle
        except Exception as e:
            Logging.error(
                f"Error while reading {self.bucket_name}/{object_name} to S3, Exception: {e}"
            )

    def write_pickle_list_s3(
        self, object_name=None, data=None, bucket_name: str = config.s3_bucket_name
    ) -> None:
        """
        Write list as pickle to S3
        :param object_name: Object path
        :param data: List to be saved
        :param bucket_name: S3 bucket name
        :return: None
        """
        try:
            Logging.info(f"Start dumping {bucket_name}/{object_name} into s3")
            byte_object = pickle.dumps(data)
            self.resource.Object(bucket_name, object_name).put(Body=byte_object)
        except Exception as e:
            Logging.error(
                f"Error while dumping {bucket_name}/{object_name} to S3, Exception: {e}"
            )

    def read_pickle_list_s3(
        self, object_name=None, bucket_name: str = config.s3_bucket_name
    ) -> list:
        """
        Read pickled list from S3
        :param object_name: S3 path of file
        :param bucket_name: S3 bucket name
        :return: list of values
        """
        try:
            Logging.info(f"Start reading {bucket_name}/{object_name} into s3")
            content_object = self.resource.Object(bucket_name, object_name)
            read_file = content_object.get()["Body"].read()
            load_pickle = pickle.loads(read_file)
            return load_pickle
        except Exception as e:
            Logging.error(
                f"Error while reading {bucket_name}/{object_name} from S3, Exception: {e}"
            )

    @classmethod
    def new_connection(cls):
        return cls.from_connection()
