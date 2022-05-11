'''
requirements:
    
gcsfs==2022.3.0
cerberus==1.3.4
google-cloud-pubsub==0.34.0
google-cloud-storage==1.17.0
'''  

# The following packages are used for the below functions:
# _make_json, _validation_failure_topic (json only)
import csv
import json
import gcsfs
# _check_validation
import cerberus
# _validation_success_topic, _validation_failure_topic
from google.cloud import pubsub_v1
# _upload_blob_from_memory, _delete_blob
from google.cloud import storage

PUBLISH_CLIENT = pubsub_v1.PublisherClient()
STORAGE_CLIENT = storage.Client()
PROJECT_ID = 'we-count-emr'
TOPIC_SUCCESS_ID = 'validation_success_topic'
TOPIC_FAILURE_ID = 'validation_failure_topic'
[SCHEMA] = [Schema] ## CODE FOR SCHEMA DEFINED HERE

def validation(data, context):
    BUCKET_NAME = data['bucket']
    FILE_NAME = data['name']
    GCS_CSV_PATH = f'gs://[name of bucket]/{FILE_NAME}' ## CHANGE [name of bucket] TO BUCKET WHERE FILE IS UPLOADED TO
    GCS_JSON_PATH = f'gs://[name of bucket]/{FILE_NAME}.json' ## CHANGE [name of bucket] TO BUCKET WHERE FILE IS UPLOADED TO
    SUCCESS_TOPIC = PUBLISH_CLIENT.topic_path(PROJECT_ID, TOPIC_SUCCESS_ID)
    FAILURE_TOPIC = PUBLISH_CLIENT.topic_path(PROJECT_ID, TOPIC_FAILURE_ID)
    ERROR_BUCKET = 'validation_error_reports'
    ERROR_REPORT = f'Validation Error Report, {FILE_NAME}'
    ## Create a json file from the data file.
    ## If the file passes validation, publish success message to PubSub.
    ## If the file fails validation, publish failure message to PubSub, upload error report to Storage, and delete data file from Storage. 
    _make_json(GCS_CSV_PATH, GCS_JSON_PATH)
    if _check_validation([SCHEMA], dataf): ## UPDATE [SCHEMA] WITH SCHEMA VARIABLE DEFINED ABOVE
        _validation_success_topic(SUCCESS_TOPIC, FILE_NAME)
        _remove_json(BUCKET_NAME)
    else:
        _validation_failure_topic(FAILURE_TOPIC, FILE_NAME)
        _upload_blob_from_memory(ERROR_BUCKET, ERROR_REPORT, error_report_full)
        _delete_blob(BUCKET_NAME, FILE_NAME)
        _remove_json(BUCKET_NAME)

def _make_json(csv_File_Path, json_File_Path):
    global dataf
    dataf = {}
    gcs_file_system = gcsfs.GCSFileSystem(project=PROJECT_ID)
    with gcs_file_system.open(csv_File_Path, 'rt', encoding='utf-8') as csvf:
        csv_Reader = csv.DictReader(csvf)
        for rows in csv_Reader:
            key = rows['row_num']
            dataf[key] = rows
    with gcs_file_system.open(json_File_Path, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(dataf, indent=4))

def _check_validation(schema, data):
    global error_messages
    len_dataf = len(dataf)
    i = 1
    error_messages = []
    while i<=len_dataf:
        _schema_demographics = {str(i):{'schema':schema}}
        v = cerberus.Validator(_schema_demographics, purge_unknown=True,require_all=True)
        v.validate(data)
        if v.errors != {}:
            error_messages.append(v.errors)
        i += 1
    if error_messages == []:
        return True
    else:
        return False

def _validation_success_topic(topic_path, _file_name):
    success_message = f'File {_file_name} was successfully validated. The file is ready for ingestion into the project database.'
    success_message_enc = success_message.encode('utf-8')
    future = PUBLISH_CLIENT.publish(topic_path, success_message_enc)
    print(future.result())
    print(success_message)

def _validation_failure_topic(topic_path, _file_name):
    global error_report_full
    list_error_messages = []
    list_error_report = []
    for i in range(len(error_messages)):
        error_dict = error_messages[i]
        list_error_messages.append(error_dict)
    for e in range(len(list_error_messages)):
        error_list = list_error_messages[e]
        to_json = json.dumps(error_list)
        list_error_report.append(to_json)
    error_report = '\n'.join(list_error_report)
    error_report_full = f'File {_file_name} failed validation. Error report:\n\n' + error_report
    error_report_bytes = str.encode(error_report_full)
    future = PUBLISH_CLIENT.publish(topic_path, error_report_bytes)
    print(future.result())
    print(f'File {_file_name} failed validation.')

def _upload_blob_from_memory(bucket_name, destination_blob_name, contents):
    bucket = STORAGE_CLIENT.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(contents)
    print('{} available in bucket: {}.'.format(destination_blob_name, bucket_name))

def _delete_blob(bucket_name, blob_name):
    bucket = STORAGE_CLIENT.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    print('File {} deleted from bucket: {}.'.format(blob_name, bucket_name))

def _remove_json(bucket_name):
    if __list_blobs(bucket_name):
        __delete_blob(bucket_name, json_blob_name)

def __list_blobs(bucket_name):
    global json_blob_name
    blobs = STORAGE_CLIENT.list_blobs(bucket_name)
    for blob in blobs:
        json_blob_name = blob.name
        if 'json' in str(json_blob_name):
            return True

def __delete_blob(bucket_name, blob_name):
    bucket = STORAGE_CLIENT.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    print('File {} deleted.'.format(blob_name))
