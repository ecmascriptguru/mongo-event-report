import datetime
import pandas as pd
from dateutil.parser import parse
from .config import *


class DataDog(object):
    """Utility to manage data resource"""
    def __init__(self, *args, **kwargs):
        self.db = db
        self.report_data = self.db[REPORT_DB_NAME]
        if ENV == ENVIRONMENT.development:
            self.prepare_example_data()

    def import_sample(self, file, encoding):
        read_map = {
            'xls': pd.read_excel, 'xlsx': pd.read_excel, 'csv': pd.read_csv,
            'gz': pd.read_csv, 'pkl': pd.read_pickle}
        ext = open(file).name.split('.')[-1]
        if read_map.get(ext, None):
            try:
                read_func = read_map.get(ext)
                content = read_func(file, encoding=encoding)

                content.columns = range(content.shape[1])
                columns = {"player": 0, "type": 1, "subtype": 2, "id": 3, "value": 4, "ts": 5}
                for col in columns:
                    content.rename(columns={content.columns[columns[col]]: col}, inplace=True)
                
                content = content[list(columns)]
                rows = [dict(row._asdict()) for row in content.itertuples()]
                # count = self.module.bulk_insert(rows, filename=secure_filename(file.filename))
                return True, "Successfully parsed the file.", rows
            except Exception as e:
                return False, str(e), []
        else:
            return False, 'Input file not in correct format, must be xls, xlsx, csv, csv.gz, pkl', []


    def prepare_example_data(self):
        _, msg, rows = self.import_sample(SAMPLE_DATA_FILE, 'UTF-8')
        if not _:
            raise Exception("Failed to load data from spreadsheet.")
        
        if not "%s.%s" %(REPORT_DB_NAME, EVENTS_TABLE_NAME) in\
                        self.db.collection_names(include_system_collections=False):
            self.events = self.report_data[EVENTS_TABLE_NAME]
            for row in rows:
                row.pop('Index')
                row['ts'] = parse(row['ts'])
            
            result = self.events.insert_many(rows)
            print("%d events were added into database successfully." % len(result.inserted_ids))
        else:
            self.events = self.report_data[EVENTS_TABLE_NAME]
