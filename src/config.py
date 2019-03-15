# MAIN CONFIG FILE
import os, json
from os.path import dirname, join, exists
from pymongo import MongoClient

class ENVIRONMENT:
    development = 'dev'
    production = 'production'

ENV = os.environ.get('REPORT_PROJECT_ENV', ENVIRONMENT.production)
BASE_DIR = dirname(dirname(__file__))


LOCAL_ENV_JSON_FILE = join(BASE_DIR, 'env.json')
SAMPLE_DATA_FILE = join(dirname(BASE_DIR), 'k.csv')
if not exists(LOCAL_ENV_JSON_FILE):
    raise Exception('LOCAL ENV file not found.')

with open(LOCAL_ENV_JSON_FILE) as data:
    ENV_JSON = json.load(data)


if not ENV_JSON.get('MONGODB_HOST') or not ENV_JSON.get('MONGODB_PORT'):
    raise Exception('DATABASE Configuration not found. Please look at readme')

db = None
try:
    client = MongoClient(ENV_JSON.get('MONGODB_HOST'), ENV_JSON.get('MONGODB_PORT'))
    db = client.admin
except Exception as e:
    raise Exception("DB Connection failed!")
    print(str(e))


REPORT_DB_NAME = ENV_JSON.get('REPORT_DB_NAME', 'report_data')
EVENTS_TABLE_NAME = ENV_JSON.get('EVENTS_TABLE_NAME', 'events')
REPORT_CONTACT_REF_FIELD_NAME = ENV_JSON.get('REPORT_CONTACT_REF_FIELD_NAME', 'contact_event_ref')
REPORT_CONTACT_TIME_DELTA_FIELD_NAME = ENV_JSON.get('REPORT_CONTACT_TIME_DELTA_FIELD_NAME', 'contact_time_delta')