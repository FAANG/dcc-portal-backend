import json
import os
import sched
import smtplib
import ssl
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from google.cloud import secretmanager
from elasticsearch import Elasticsearch, RequestsHttpConnection, exceptions
import logging

# logging options
logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger()

fileHandler = logging.FileHandler("{0}/{1}.log".format('./', 'snapshot_comparison'))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

load_dotenv()

# get env variables
PROJECT = os.getenv("PROJECT")
ES_HOST = os.getenv("ES_NODE")


# get password stored in GCP secrets
def get_secret_data(project_id, secret_id, version_id):
    client = secretmanager.SecretManagerServiceClient()
    secret_detail = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": secret_detail})
    data = response.payload.data.decode("UTF-8")
    return data


# get ES credentials
es_user = get_secret_data(PROJECT, 'ES_USER', 1)
es_password = get_secret_data(PROJECT, 'ES_PASSWORD', 1)

snapshot_repository = "found-snapshots"

# connect to ES cluster
es = Elasticsearch([ES_HOST],
                   connection_class=RequestsHttpConnection,
                   http_auth=(es_user, es_password),
                   use_ssl=True, verify_certs=False,
                   ssl_show_warn=False)


def get_snapshots():
    all_snapshots = es.snapshot.get(repository=snapshot_repository, snapshot='_all')
    # snapshot sorted by default in ascending order by id
    new_snapshot = all_snapshots['snapshots'][-1:][0]['snapshot']
    old_snapshot = all_snapshots['snapshots'][0]['snapshot']
    return [new_snapshot, old_snapshot]


def restore_snapshot(indices):
    new_snapshot, old_snapshot = get_snapshots()
    # restore latest snapshot databases
    new_snapshot_settings = {
        "indices": indices,
        "rename_pattern": "(.+)",
        "rename_replacement": "restored-new-snapshot-$1"
    }
    es.snapshot.restore(repository=snapshot_repository, snapshot=new_snapshot, body=new_snapshot_settings)

    # restore oldest snapshot databases
    old_snapshot_settings = {
        "indices": indices,
        "rename_pattern": "(.+)",
        "rename_replacement": "restored-old-snapshot-$1"
    }
    es.snapshot.restore(repository=snapshot_repository, snapshot=old_snapshot, body=old_snapshot_settings)
    logger.info(f'Restoring snapshots {new_snapshot} and {old_snapshot}')


def delete_restored_indices():
    es.indices.delete(index='restored-new-snapshot-*')
    es.indices.delete(index='restored-old-snapshot-*')
    logger.info(f'Deleting restored snapshots')


def get_primary_key(key):
    indices_keys = {
        "dataset": "accession",
        "article": "doi",  # pmcId
        "analysis": "accession",
        "organism": "biosampleId",
        "specimen": "biosampleId",
        "file": "name",
        "protocol_samples": "key",
        "protocol_files": "key",
        "protocol_analysis": "key",
    }
    return indices_keys[key]


def create_transform(index):
    es_transform_id = f"{index}_comparison_transform"
    logger.info(f'Creating transform {es_transform_id}')
    create_transform_settings = {
        "id": es_transform_id,
        "source": {
            "index": [
                f"restored-old-snapshot-{index}",
                f"restored-new-snapshot-{index}"
            ],
            "query": {
                "match_all": {}
            }
        },
        "dest": {
            "index": f"compare-index-{index}"
        },
        "pivot": {
            "group_by": {
                "unique-id": {
                    "terms": {
                        "field": get_primary_key(index)
                    }
                }
            },
            "aggregations": {
                "compare": {
                    "scripted_metric": {
                        "map_script": "state.doc = new HashMap(params['_source'])",
                        "combine_script": "return state",
                        "reduce_script": "if (states.size() != 2) {return \"count_mismatch\"}"
                                         "if (states.get(0).equals(states.get(1))) { return \"match\"}"
                                         "else { return \"document_mismatch\" }"
                    }
                }
            }
        }
    }
    # create transform
    es.transform.put_transform(es_transform_id, body=create_transform_settings)


def run_transform(indices):
    for index in indices:
        try:
            # Run existing transform if it exists
            es_transform_id = f"{index}_comparison_transform"
            existing_transform = es.transform.get_transform(es_transform_id)
            if existing_transform['count'] > 0:
                logger.info(f'Running tranform {es_transform_id}')
                es.transform.start_transform(es_transform_id)
        except exceptions.NotFoundError:
            create_transform(index)
            es.transform.start_transform(es_transform_id)
        except Exception as e:
            es.transform.delete_transform(es_transform_id)
            create_transform(index)
            es.transform.start_transform(es_transform_id)


def stop_reset_transform(indices):
    s = sched.scheduler(time.time, time.sleep)

    def reset_transform(sc, es_transform_id):
        transform_stats = es.transform.get_transform_stats(es_transform_id)
        if transform_stats['transforms'][0]['state'] == 'stopped':
            cmd_reset_transform = f"curl -k -X POST '{ES_HOST}/_transform/{es_transform_id}/_reset?pretty' " \
                                  f"-u {es_user}:{es_password}"
            os.system(cmd_reset_transform)
            return True
        sc.enter(60, 1, reset_transform, (sc, es_transform_id))

    for index in indices:
        try:
            # stop transform
            es_transform_id = f"{index}_comparison_transform"
            es.transform.stop_transform(es_transform_id)
            # reset transform
            s.enter(60, 1, reset_transform, (s, es_transform_id))
            s.run()
        except exceptions.NotFoundError:
            continue


def get_transforms_status(indices):
    s = sched.scheduler(time.time, time.sleep)
    transform_status_dict = dict.fromkeys(indices)

    def get_transform_statistics(sc, index_name):
        es_transform_id = f"{index_name}_comparison_transform"
        transform_stats = es.transform.get_transform_stats(es_transform_id)

        if transform_stats['transforms'][0]['state'] == 'stopped':
            transform_status_dict[index_name] = 'stopped'
            return True
        if transform_stats['transforms'][0]['state'] == 'failed':
            transform_status_dict[index_name] = 'failed'
            return True
        sc.enter(60, 1, get_transform_statistics, (sc, index_name))

    for index in indices:
        s.enter(60, 1, get_transform_statistics, (s, index))
        s.run()

    return transform_status_dict


def get_mismatched_docs(index):
    filters = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"compare": "document_mismatch"}},
                    {"match": {"compare": "count_mismatch"}}
                ]
            }
        }
    }
    query = json.dumps(filters)
    data = es.search(index=f"compare-index-{index}",
                     size=10000,
                     from_=0,
                     track_total_hits=True,
                     body=json.loads(query))
    return data['hits']['hits']


def get_subscribers(index):
    res = es.search(index="faang_subscriptions", size=1, from_=0,
                    track_total_hits=True)
    if "subscribers" in res['hits']['hits'][0]['_source']['index_subscribers'][index]:
        subscribers = res['hits']['hits'][0]['_source']['index_subscribers'][index]['subscribers']
        subscribers_emails = [d.get('email', None) for d in subscribers]
        return subscribers_emails


def send_email(index, count_mismatch_docs, document_mismatch_docs, subscribers_emails):
    port = 587  # For starttls
    smtp_server = "outgoing.ebi.ac.uk"
    sender_email = "email_host_username"
    password = "email_host_password"

    for email in subscribers_emails:
        message = MIMEMultipart("alternative")
        message["Subject"] = f"Updates about FAANG {index} entries"
        message["From"] = sender_email
        message["To"] = email

        # Create HTML version of your message
        faang_frontend = 'https://data.faang.org/'
        count_mismatch_html_str = ''
        doc_mismatch_html_str = ''

        for ele in count_mismatch_docs:
            record_link = f"{faang_frontend}{index}/{ele['unique-id']}"
            count_mismatch_html_str += f"<p><a href='{record_link}' style='color: steelblue;'>{ele['unique-id']}</a></p>"
        count_mismatch_html_str = "<p> Addition/Deletion of documents </p>" + count_mismatch_html_str

        for ele in document_mismatch_docs:
            record_link = f"{faang_frontend}{index}/{ele['unique-id']}"
            doc_mismatch_html_str += f"<p><a href='{record_link}' style='color: steelblue;'>{ele['unique-id']}</a></p>"
        doc_mismatch_html_str = "<p> Changes in document contents </p>" + doc_mismatch_html_str

        html = f"""\
                <html>
                  <body style='color: #373737;'>
                    <p>Please see below for the most recent updates in FAANG's database about the data you have 
                    registered for.
                    {count_mismatch_html_str if count_mismatch_docs else ""}
                    {doc_mismatch_html_str if document_mismatch_docs else ""}
                    </p>
                  </body>
                </html>
                """

        # Turn these into plain/html MIMEText objects
        part2 = MIMEText(html, "html")
        message.attach(part2)

        # Create a secure SSL context
        # context = ssl.create_default_context()
        context = ssl._create_unverified_context()

        # Try to log in to server and send email
        try:
            server = smtplib.SMTP(smtp_server, port)
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(sender_email, password)
            server.sendmail(sender_email, email, message.as_string())
            logger.info(f'Email sent to {email}')
            print("Email sent")
        except Exception as e:
            print(e)
        finally:
            server.quit()


if __name__ == '__main__':
    indices_str = 'protocol_analysis,file,protocol_files,protocol_samples,specimen,organism,analysis,article,dataset'
    indices_list = indices_str.split(',')

    # Step 1: restore snapshot indices
    delete_restored_indices()
    restore_snapshot(indices_str)

    # Step 2: reset transform - all checkpoints, states, and the destination index are deleted
    stop_reset_transform(indices_list)

    # Step 3: Start transform process - creates transform if it does not exist, otherwise just start the existing
    # transform.
    # The destination index is recreated
    run_transform(indices_list)

    # Step 4: Check transform statuses for all transforms created
    transform_status_dict = get_transforms_status(indices_list)

    # transform has finished running for all indices
    if all(value == 'stopped' for value in transform_status_dict.values()):
        # for each index, get the mismatched documents and email the ids to subscribers
        for index in indices_list:
            data = get_mismatched_docs(index)
            count_mismatch_docs = [ele['_source'] for ele in data
                                   if ele['_source']['compare'] == 'count_mismatch']
            document_mismatch_docs = [ele['_source'] for ele in data
                                      if ele['_source']['compare'] == 'document_mismatch']

            # get list of subscribers
            subscribers_emails = get_subscribers(index)
            if subscribers_emails and (count_mismatch_docs or document_mismatch_docs):
                send_email(index, count_mismatch_docs, document_mismatch_docs, subscribers_emails)
    else:
        print(
            f"We had an issue running the transforms for indices "
            f"{[k for k, v in transform_status_dict.items() if v == 'failed']}")

    # Step5: After transform process has completed, delete the restored snapshot indices
    delete_restored_indices()
