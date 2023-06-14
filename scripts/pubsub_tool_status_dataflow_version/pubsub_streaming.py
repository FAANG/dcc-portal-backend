from time import localtime, strftime
import logging
import apache_beam as beam
from dotenv import load_dotenv
from apache_beam import io, ParDo, Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
import json
import os
from google.cloud import secretmanager

# get env variables
load_dotenv()
ES_HOST = os.getenv("ES_NODE")
PROJECT = os.getenv("PROJECT")
REPO = os.getenv("REPO")
IMAGE = os.getenv("IMAGE")
REGION = os.getenv("REGION")


def get_secret_data(project_id, secret_id, version_id):
    client = secretmanager.SecretManagerServiceClient()
    secret_detail = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": secret_detail})
    data = response.payload.data.decode("UTF-8")
    print("get_secret_data: ", data)
    return data


# get ES credentials
es_user = get_secret_data(PROJECT, 'ES_USER', 1)
es_password = get_secret_data(PROJECT, 'ES_PASSWORD', 1)


def es_instance():
    es = Elasticsearch([ES_HOST],
                       connection_class=RequestsHttpConnection,
                       http_auth=(es_user, es_password),
                       use_ssl=True,
                       verify_certs=True,
                       ssl_show_warn=False)
    return es


def get_submission_tool_status(project_state):
    if project_state == 'open':
        return 'failure'
    elif project_state == 'closed':
        return 'success'
    return None


def current_submission_portal_status_entry():
    filters = {"ids": {"values": [1]}}
    query = json.dumps(filters)
    es = es_instance()
    res = es.search(index='submission_portal_status', size=1, from_=0,
                    track_total_hits=True, query=json.loads(query))
    if len(res['hits']['hits']) > 0:
        return res['hits']['hits'][0]['_source']


class GetToolStatus(beam.DoFn):
    def process(self, element):
        json_object = json.loads(element)
        project_tag = json_object["incident"]["policy_user_labels"]["application_tag"].lower()
        project_state = json_object["incident"]["state"].lower()

        update_flag = False
        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        doc_dict = current_submission_portal_status_entry()
        submission_tool_status = get_submission_tool_status(project_state)
        if 'biosamples' in project_tag and submission_tool_status is not None:
            doc_dict["biosample_status"] = submission_tool_status
            doc_dict["last_updated"] = current_time
            update_flag = True

        if 'ena' in project_tag and submission_tool_status is not None:
            doc_dict["ena_status"] = submission_tool_status
            doc_dict["last_updated"] = current_time
            update_flag = True

        if update_flag:
            es = es_instance()
            res = es.update(index="submission_portal_status", id=1, doc=doc_dict)
            print(res)


def run():
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        project=PROJECT,
        region=REGION,
        runner='DataflowRunner',
        experiments=['use_runner_v2'],
        sdk_container_image=f'europe-west2-docker.pkg.dev/{PROJECT}/{REPO}/{IMAGE}:tag1',
        sdk_location='container',
        streaming=True,
        save_main_session=True
    )

    input_subscription = 'projects/prj-ext-dev-faang-gcp-dr/subscriptions/faang-new-test-sub'

    with Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub" >> io.ReadFromPubSub(subscription=input_subscription).with_output_types(bytes)
            | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
            | 'get tool status' >> ParDo(GetToolStatus())
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
