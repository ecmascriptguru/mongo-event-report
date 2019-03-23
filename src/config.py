# MAIN CONFIG FILE
import os, json
from os.path import dirname, join, exists
from pymongo import MongoClient

class ENVIRONMENT:
    development = 'dev'
    production = 'production'


class COMMAND_OPTIONS:
    preprocess = '--pre'
    all = '--all'
    date = '--date'

class COMMAND_DATE_OPTIONS:
    today = '--today'
    yesterday = '--yesterday'


ENV = os.environ.get('REPORT_PROJECT_ENV', ENVIRONMENT.production)
BASE_DIR = dirname(dirname(__file__))


LOCAL_ENV_JSON_FILE = join(BASE_DIR, 'env.json')

if not exists(LOCAL_ENV_JSON_FILE):
    raise Exception('LOCAL ENV file not found.')

with open(LOCAL_ENV_JSON_FILE) as data:
    ENV_JSON = json.load(data)


if not ENV_JSON.get('MONGODB_HOST') or not ENV_JSON.get('MONGODB_PORT'):
    raise Exception('DATABASE Configuration not found. Please look at readme')

# Setting the sample csv file to test functionalities
# The file should be placed in parent folder of this project path
SAMPLE_DATA_FILE_NAME = ENV_JSON.get('SAMPLE_DATA_FILE_NAME', 'k.csv')
SAMPLE_DATA_FILE = join(dirname(BASE_DIR), SAMPLE_DATA_FILE_NAME)

db = None
DB_NAME = ENV_JSON.get('DB_NAME', 'admin')
EVENTS_TABLE_NAME = ENV_JSON.get('EVENTS_TABLE_NAME', 'report_data.events')
CONTACTS_REPORT_RESULTS_TABLE_NAME = ENV_JSON.get('CONTACTS_REPORT_RESULTS_TABLE_NAME', 'report_data.reports')
FREEBIE_REPORT_RESULTS_TABLE_NAME = ENV_JSON.get('FREEBIE_REPORT_RESULTS_TABLE_NAME', 'report_data.freebies')
REPORT_CONTACT_REF_FIELD_NAME = ENV_JSON.get('REPORT_CONTACT_REF_FIELD_NAME', 'contact_event_ref')
REPORT_CONTACT_REF_SUBTYPE_FIELD_NAME = ENV_JSON.get('REPORT_CONTACT_REF_SUBTYPE_FIELD_NAME', 'contact_event_subtype')
REPORT_CONTACT_TIME_DELTA_FIELD_NAME = ENV_JSON.get('REPORT_CONTACT_TIME_DELTA_FIELD_NAME', 'contact_time_delta')
REPORT_ANALYSIS_TIME_WINDOW = ENV_JSON.get('REPORT_ANALYSIS_TIME_WINDOW', 10)

try:
    client = MongoClient(ENV_JSON.get('MONGODB_HOST'), ENV_JSON.get('MONGODB_PORT'))
    db = client[DB_NAME]
except Exception as e:
    raise Exception("DB Connection failed!")
    print(str(e))
