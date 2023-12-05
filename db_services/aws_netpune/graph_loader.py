import json
import time
from http import HTTPStatus

from rplus_constants import IAMROLE_ID, LOAD_ID, PAYLOAD, OVERALLSTATUS, STATUS, LOAD_FAILED, LOAD_IN_QUEUE, \
    LOAD_COMPLETED

from rplus_utils.common.config import config
from rplus_utils.db_services.aws_netpune.rest import Rest
from rplus_utils.logger import Logging


class GenerateNode:

    @staticmethod
    def create_node(limit=None, key=None, url=config.graph_loader_uri):
        """
        :param limit:length of dataframe
        :param key: s3_object_name
        :param url: graph url
        :return: dump on graph
        """
        rest = Rest()
        # Dump to AN
        payload = {
            "source": "s3://{}/{}".format(config.s3_bucket_name, key),
            "format": "csv",
            "mode": "AUTO",
            "iamRoleArn": "arn:aws:iam::{}:role/NeptuneAccessS3".format(IAMROLE_ID),
            "region": config.region_name,
            "failOnError": "FALSE",
            "parallelism": "MEDIUM",
            "queueRequest": "TRUE",
            "updateSingleCardinalityProperties": "FALSE",
        }
        resp = rest.post(url, payload=payload)
        time.sleep(2)
        if resp.status_code != HTTPStatus.OK:
            raise Exception("Error to post neptune -> ", resp.json())
        response = resp.json()
        queue_id = response[PAYLOAD][LOAD_ID]

        for iteration in range(limit):
            temp_url = "{}/{}".format(url, queue_id)
            resp = rest.get(temp_url)
            if resp.status_code != HTTPStatus.OK:
                raise Exception("error fetching status neptune")

            time.sleep(1)
            response = resp.json()

            Logging.info("status neptune -> {}".format(json.dumps(response)))
            if response[PAYLOAD][OVERALLSTATUS][STATUS] not in [LOAD_COMPLETED, LOAD_FAILED, LOAD_IN_QUEUE]:
                time.sleep(3)

            else:
                break

        return response[PAYLOAD][OVERALLSTATUS][STATUS]

    @staticmethod
    def upsert_node(limit=None, key=None, url=config.graph_loader_uri):
        """
        :param limit:lenght of dataframe
        :param key: s3_object_key_name
        :param url: graph_loader
        :return: dump on s3
        """

        rest = Rest()
        # Dump to AN
        payload = {
            "source": "s3://{}/{}".format(config.s3_bucket_name, key),
            "format": "csv",
            "mode": "AUTO",
            "iamRoleArn": "arn:aws:iam::{}:role/NeptuneAccessS3".format(IAMROLE_ID),
            "region": config.region_name,
            "failOnError": "FALSE",
            "parallelism": "MEDIUM",
            "updateSingleCardinalityProperties": "TRUE",
        }
        resp = rest.post(url, payload=payload)
        time.sleep(2)
        if resp.status_code != HTTPStatus.OK:
            raise Exception("Error to post neptune -> ", resp.json())
        response = resp.json()
        queue_id = response[PAYLOAD][LOAD_ID]

        for iteration in range(limit):
            temp_url = "{}/{}".format(url, queue_id)
            resp = rest.get(temp_url)
            if resp.status_code != HTTPStatus.OK:
                raise Exception("error fetching status neptune")

            time.sleep(1)
            response = resp.json()

            Logging.info("status neptune -> {}".format(json.dumps(response)))

            if response[PAYLOAD][OVERALLSTATUS][STATUS] not in [LOAD_COMPLETED, LOAD_FAILED, LOAD_IN_QUEUE]:
                time.sleep(3)

            else:
                if response[PAYLOAD][OVERALLSTATUS][STATUS] == LOAD_FAILED:
                    Logging.error("status neptune -> {}".format(json.dumps(response)))
                    break
                break

        return response[PAYLOAD][OVERALLSTATUS][STATUS]
