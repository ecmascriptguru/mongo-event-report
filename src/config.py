# MAIN CONFIG FILE
from os.path import dirname, join


ENV = os.environ.get('REPORT_PROJECT_ENV', 'prod')
BASE_DIR = dirname(dirname(__file__))