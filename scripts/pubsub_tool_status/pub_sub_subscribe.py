import os
from concurrent.futures._base import TimeoutError
from google.pubsub_v1 import PubsubMessage
from google.cloud.pubsublite.cloudpubsub import SubscriberClient
from google.cloud.pubsublite.types import (
    CloudRegion,
    CloudZone,
    FlowControlSettings,
    SubscriptionPath,
)
from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from dotenv import load_dotenv
import json

# load .env variables
load_dotenv()

# read from k8s secrets
ES_USERNAME = os.getenv('ES_USER')
ES_PASSWORD = os.getenv('ES_PASSWORD')
GCP_CREDENTIALS = os.getenv('prj-ext-dev-faang-gcp-dr-6712304182f1')

ES_HOST = os.environ.get("ES_NODE")


# connect to ES cluster
es = Elasticsearch([ES_HOST],
                   connection_class=RequestsHttpConnection,
                   http_auth=(ES_USERNAME, ES_PASSWORD),
                   use_ssl=True, verify_certs=False,
                   ssl_show_warn=False)


def get_pub_sub_messages():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/etc/gcp/service-account-credentials.json'

    project_number = 964531885708
    cloud_region = "europe-west2"
    zone_id = "a"
    subscription_id = "faang-test-sub"
    timeout = 90
    regional = True

    if regional:
        location = CloudRegion(cloud_region)
    else:
        location = CloudZone(CloudRegion(cloud_region), zone_id)

    subscription_path = SubscriptionPath(project_number, location, subscription_id)
    per_partition_flow_control_settings = FlowControlSettings(
        messages_outstanding=1000,
        bytes_outstanding=10 * 1024 * 1024,
    )

    with SubscriberClient() as subscriber_client:
        streaming_pull_future = subscriber_client.subscribe(
            subscription_path,
            callback=callback,
            per_partition_flow_control_settings=per_partition_flow_control_settings,
        )
        print(f"Listening for messages on {str(subscription_path)}...")

        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError or KeyboardInterrupt:
            streaming_pull_future.cancel()
            assert streaming_pull_future.done()


def callback(message: PubsubMessage):
    message_data = message.data.decode("utf-8")
    print(message_data)

    update_flag = False
    biosample_tool_status = get_submission_tool_status('biosample', message_data.lower())
    ena_tool_status = get_submission_tool_status('ena', message_data.lower())

    doc_dict = current_submission_portal_status_entry()
    if biosample_tool_status is not None:
        doc_dict["biosample_status"] = biosample_tool_status
        update_flag = True
    if ena_tool_status is not None:
        doc_dict["ena_status"] = ena_tool_status
        update_flag = True

    if update_flag:
        res = es.update(index="submission_portal_status", id=1, doc=doc_dict)
        print(res)

    message.ack()


def get_submission_tool_status(archive, message_data):
    if all(map(message_data.__contains__, ["failure", archive])):
        return 'failure'
    elif all(map(message_data.__contains__, ["success", archive])):
        return 'success'
    return None


def current_submission_portal_status_entry():
    filters = {"ids": {"values": [1]}}
    query = json.dumps(filters)
    res = es.search(index='submission_portal_status', size=1, from_=0,
                    track_total_hits=True, query=json.loads(query))
    if len(res['hits']['hits']) > 0:
        return res['hits']['hits'][0]['_source']


if __name__ == '__main__':
    get_pub_sub_messages()

