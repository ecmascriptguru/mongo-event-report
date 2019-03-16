from datetime import date, timedelta
import pandas as pd
from bson.son import SON
from dateutil.parser import parse
from .config import *


class TYPE:
    contact = u"contact"
    login = u"login"
    deposit = u"deposit"


class SUB_TYPE:
    contact_email = u"email"
    contact_sms = u"sms"
    contact_notification = u"notification"


SUB_TYPES = {
    TYPE.contact: [SUB_TYPE.contact_email, SUB_TYPE.contact_sms, SUB_TYPE.contact_notification, ],
    TYPE.login: [],
    TYPE.deposit: []
}


class DataDog(object):
    """Utility to manage data resource"""
    is_preprocessed = False

    def __init__(self, *args, **kwargs):
        self.db = db
        if ENV == ENVIRONMENT.development:
            self.prepare_example_data()
        else:
            if not EVENTS_TABLE_NAME in\
                self.db.collection_names(include_system_collections=False):
                raise Exception("Collections not found. You may need to check"
                "'env.json' file in project folder.")
            else:
                self.events = self.db[EVENTS_TABLE_NAME]

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
        
        if not EVENTS_TABLE_NAME in\
                    self.db.collection_names(include_system_collections=False):
            self.events = self.db[EVENTS_TABLE_NAME]
            for row in rows:
                row.pop('Index')
                row['ts'] = parse(row['ts'])
            
            result = self.events.insert_many(rows)
            print("%d events were added into database successfully." % len(result.inserted_ids))
        else:
            self.events = self.db[EVENTS_TABLE_NAME]

    def assign_contact_ref(self):
        cursor = self.events.find({
            REPORT_CONTACT_REF_FIELD_NAME: { "$exists": False },
            "type": { "$in": ['login', 'deposit'] }
        })
        for doc in cursor:
            # login or deposit events
            contact_ref = self.events.find_one({
                    'player': doc['player'],
                    'type': 'contact',
                    'subtype': { '$nin': ['notification'] },
                    'ts': {'$lt': doc['ts']}
                },
                sort=[('ts', -1)])
            if contact_ref:
                doc[REPORT_CONTACT_REF_FIELD_NAME] = contact_ref['id']
                doc[REPORT_CONTACT_TIME_DELTA_FIELD_NAME] =\
                    int((doc['ts'] - contact_ref['ts']).total_seconds() / (60 * 60))
                self.events.save(doc)
        self.is_preprocessed = True
        return True

    def get_report(self, event_id, date=None):
        if date is None:
            yesterday = date.today() - timedelta(1)
            date = yesterday.strftime('%Y-%m-%d')

        if not self.is_preprocessed:
            self.assign_contact_ref()
        
        reports = self.preprocess(event_id, date)
        if len(reports) > 0:
            return reports[0]
        else:
            return None
            

    def preprocess(self, event_id, date):
        pipeline = [
            {
                u"$addFields": {
                    u"date": {
                        u"$dateToString": {
                            u"format": u"%Y-%m-%d",
                            u"date": u"$ts"
                        }
                    }
                }
            }, 
            {
                u"$match": {
                    u"type": u"contact",
                    u"subtype": {
                        u"$nin": [
                            SUB_TYPE.contact_notification
                        ]
                    },
                    u"date": date,
                    u"id": event_id
                }
            }, 
            {
                u"$group": {
                    u"_id": u"$id",
                    u"contacts": {
                        u"$sum": 1.0
                    }
                }
            }, 
            {
                u"$lookup": {
                    u"from": u"test_data.events",
                    u"let": {
                        u"event_id": u"$_id"
                    },
                    u"pipeline": [
                        {
                            u"$match": {
                                u"$and": [
                                    {
                                        u"$expr": {
                                            u"$eq": [
                                                u"$$CURRENT.contact_event_ref",
                                                u"$$event_id"
                                            ]
                                        }
                                    },
                                    {
                                        u"$expr": {
                                            u"$eq": [
                                                u"$$CURRENT.type",
                                                TYPE.login
                                            ]
                                        }
                                    },
                                    {
                                        u"$expr": {
                                            u"$lt": [
                                                u"$$CURRENT.contact_time_delta",
                                                REPORT_ANALYSIS_TIME_WINDOW
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            u"$group": {
                                u"_id": u"$$CURRENT.player",
                                u"count": {
                                    u"$sum": 1.0
                                }
                            }
                        }
                    ],
                    u"as": u"logins"
                }
            }, 
            {
                u"$lookup": {
                    u"from": u"test_data.events",
                    u"let": {
                        u"event_id": u"$_id"
                    },
                    u"pipeline": [
                        {
                            u"$match": {
                                u"$and": [
                                    {
                                        u"$expr": {
                                            u"$eq": [
                                                u"$$CURRENT.contact_event_ref",
                                                u"$$event_id"
                                            ]
                                        }
                                    },
                                    {
                                        u"$expr": {
                                            u"$eq": [
                                                u"$$CURRENT.type",
                                                TYPE.deposit
                                            ]
                                        }
                                    },
                                    {
                                        u"$expr": {
                                            u"$lt": [
                                                u"$$CURRENT.contact_time_delta",
                                                REPORT_ANALYSIS_TIME_WINDOW
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            u"$group": {
                                u"_id": u"$$CURRENT.player",
                                u"count": {
                                    u"$sum": 1.0
                                },
                                u"sum": {
                                    u"$sum": u"$$CURRENT.value"
                                },
                                u"avg": {
                                    u"$avg": u"$$CURRENT.value"
                                }
                            }
                        }
                    ],
                    u"as": u"deposits"
                }
            }, 
            {
                u"$project": {
                    u"_id": 0,
                    u"id": u"$_id",
                    u"date": date,
                    u"contacts": u"$contacts",
                    u"loggedin_players": {
                        u"$size": u"$logins"
                    },
                    u"deposits": {
                        u"$sum": u"$deposits.count"
                    },
                    u"depositors": {
                        u"$size": u"$deposits"
                    },
                    u"totalvalue": {
                        u"$sum": u"$deposits.sum"
                    },
                    u"meanvalue_per_user": {
                        u"$ifNull": [
                            {
                                u"$avg": u"$deposits.avg"
                            },
                            0.0
                        ]
                    },
                    u"meanvalue": {
                        u"$cond": {
                            u"if": {
                                u"$eq": [
                                    {
                                        u"$sum": u"$deposits.count"
                                    },
                                    0.0
                                ]
                            },
                            u"then": 0.0,
                            u"else": {
                                u"$divide": [
                                    {
                                        u"$sum": u"$deposits.sum"
                                    },
                                    {
                                        u"$sum": u"$deposits.count"
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        ]

        cursor = self.events.aggregate(
            pipeline, allowDiskUse = True
        )
        return [doc for doc in cursor]