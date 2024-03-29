from datetime import date, timedelta
import pandas as pd
from bson.son import SON
from dateutil.parser import parse
from .constants import TYPE, SUB_TYPE, REPORT_TYPE
from .config import *


SUB_TYPES = {
    TYPE.contact: [SUB_TYPE.contact_email, SUB_TYPE.contact_sms, SUB_TYPE.contact_notification, ],
    TYPE.login: [],
    TYPE.deposit: []
}


class DataDog(object):
    """Utility to manage data resource"""
    is_preprocessed = False
    possible_keys_function_name_template = "get_%s_report_keys"
    report_all_func_name_template = "%s_report_for_all_data"
    report_func_name_template = "get_%s_report"

    def __init__(self, *args, **kwargs):
        self.db = db
        self.contacts_report = self.db[CONTACTS_REPORT_RESULTS_TABLE_NAME]
        self.freebies_report = self.db[FREEBIE_REPORT_RESULTS_TABLE_NAME]
        if ENV == ENVIRONMENT.development:
            self.prepare_example_data()
        else:
            if not EVENTS_TABLE_NAME in\
                self.db.collection_names(include_system_collections=False):
                raise Exception("Collections not found. You may need to check"
                "'env.json' file in project folder.")
            else:
                self.events = self.db[EVENTS_TABLE_NAME]
    
    @classmethod
    def yesterday(cls):
        yesterday = date.today() - timedelta(1)
        return yesterday.strftime('%Y-%m-%d')
    
    @classmethod
    def today(cls):
        today = date.today()
        return today.strftime('%Y-%m-%d')

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
                doc[REPORT_CONTACT_REF_SUBTYPE_FIELD_NAME] = contact_ref['subtype']
                doc[REPORT_CONTACT_TIME_DELTA_FIELD_NAME] =\
                    int((doc['ts'] - contact_ref['ts']).total_seconds() / (60 * 60))
                self.events.save(doc)
        self.is_preprocessed = True
        return True
    
    def get_possible_report_keys(self, event_date=None, report_type=REPORT_TYPE.contacts_report):
        func_name = self.possible_keys_function_name_template % report_type
        if hasattr(self, func_name):
            return getattr(self, func_name)(event_date)
        else:
            raise Exception("Not Implemented for %s type" % report_type)
    
    def get_contacts_report_keys(self, event_date=None):
        collection = self.events

        if event_date is not None:
            pipeline = [
                {
                    "$match": {
                        "type": TYPE.contact,
                        "subtype": {
                            "$nin": [
                                SUB_TYPE.contact_notification
                            ]
                        },
                    }
                }, 
                {
                    "$addFields": {
                        "date": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$ts"
                            }
                        }
                    }
                }, 
                {
                    "$match": {
                        "date": event_date,
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "id": "$id",
                            "date": "$date",
                            "subtype": "$subtype",
                        },
                        "contacts": { "$sum": 1 }
                    }
                }, 
                {
                    "$project": {
                        "_id": 0.0,
                        "id": "$_id.id",
                        "date": "$_id.date",
                        "subtype": "$_id.subtype",
                        "contacts": "$contacts",
                    }
                }
            ]
        else:
            pipeline = [
                {
                    "$match": {
                        "type": TYPE.contact,
                        "subtype": {
                            "$nin": [
                                SUB_TYPE.contact_notification
                            ]
                        }
                    }
                }, 
                {
                    "$addFields": {
                        "date": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$ts"
                            }
                        }
                    }
                }, 
                {
                    "$group": {
                        "_id": {
                            "id": "$id",
                            "date": "$date",
                            "subtype": "$subtype",
                        },
                        "contacts": { "$sum": 1 }
                    }
                }, 
                {
                    "$project": {
                        "_id": 0.0,
                        "id": "$_id.id",
                        "date": "$_id.date",
                        "subtype": "$_id.subtype",
                        "contacts": "$contacts",
                    }
                }
            ]

        cursor = collection.aggregate(
            pipeline, 
            allowDiskUse = True
        )
        return [(doc['id'], doc['subtype'], doc['date'], doc['contacts']) for doc in cursor]
    
    def get_freebies_report_keys(self, event_date=None):
        collection = self.events

        if event_date is not None:
            pipeline = [
                {
                    "$match": {
                        "type": TYPE.freebie,
                        "subtype": SUB_TYPE.freebie_coin,
                    }
                }, 
                {
                    "$addFields": {
                        "date": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$ts"
                            }
                        }
                    }
                }, 
                {
                    "$match": {
                        "date": event_date,
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "id": "$id",
                            "date": "$date",
                            "subtype": "$subtype",
                        },
                    }
                }, 
                {
                    "$project": {
                        "_id": 0.0,
                        "id": "$_id.id",
                        "date": "$_id.date",
                        "subtype": "$_id.subtype",
                    }
                }
            ]
        else:
            pipeline = [
                {
                    "$match": {
                        "type": TYPE.freebie,
                        "subtype": SUB_TYPE.freebie_coin,
                    }
                }, 
                {
                    "$addFields": {
                        "date": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$ts"
                            }
                        }
                    }
                }, 
                {
                    "$group": {
                        "_id": {
                            "id": "$id",
                            "date": "$date",
                            "subtype": "$subtype",
                        },
                    }
                }, 
                {
                    "$project": {
                        "_id": 0.0,
                        "id": "$_id.id",
                        "date": "$_id.date",
                        "subtype": "$_id.subtype",
                    }
                }
            ]

        cursor = collection.aggregate(
            pipeline, 
            allowDiskUse = True
        )
        return [(doc['id'], doc['subtype'], doc['date']) for doc in cursor]
    
    def report_for_all_data(self, event_date=None, report_type=REPORT_TYPE.contacts_report):
        func_name = self.report_all_func_name_template % report_type
        if hasattr(self, func_name):
            return getattr(self, func_name)(event_date)
        else:
            raise Exception("Not Implemented for %s type" % report_type)

    def contacts_report_for_all_data(self, event_date):
        count = 0
        keys = self.get_possible_report_keys(event_date)
        for event_id, subtype, event_date, contacts in keys:
            count += self.get_report(event_id, subtype, event_date, contacts)

        return count

    def freebies_report_for_all_data(self, event_date):
        count = 0
        keys = self.get_possible_report_keys(event_date, report_type=REPORT_TYPE.freebies_report)
        for event_id, subtype, event_date in keys:
            count += self.get_report(event_id, subtype, event_date, report_type=REPORT_TYPE.freebies_report)

        return count

    def get_contact_count(self, event_id, subtype, event_date):
        pipeline = [
                {
                    "$match": {
                        "type": "contact",
                        "subtype": subtype,
                        "id": event_id
                    }
                }, 
                {
                    "$addFields": {
                        "date": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$ts"
                            }
                        }
                    }
                }, 
                {
                    "$match": {
                        "date": event_date
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "id": "$id",
                            "date": "$date",
                            "subtype": "$subtype",
                        },
                        "contacts": { "$sum": 1 }
                    }
                }, 
                {
                    "$project": {
                        "contacts": { "$ifNull": ["$contacts", 0] },
                    }
                }
            ]

        cursor = self.events.aggregate(
            pipeline, 
            allowDiskUse = True
        )

        for doc in cursor:
            return doc['contacts']

    def get_report(self, event_id, subtype=None, event_date=None, contacts=None, 
                    report_type=REPORT_TYPE.contacts_report):
        func_name = self.report_func_name_template % report_type
        if hasattr(self, func_name):
            return getattr(self, func_name)(event_id, subtype, event_date, contacts)
        else:
            raise Exception("Not implemented yet.")

    def get_contacts_report(self, event_id, subtype=None, event_date=None, contacts=None):
        count = 0
        if event_date is None:
            event_date = self.yesterday()

        if contacts is None:
            contacts = self.get_contact_count(event_id, subtype, event_date)

        reports = self.create_contact_report(event_id, subtype, event_date, contacts)
        for report in reports:
            self.contacts_report.update({
                    'id': event_id,
                    'subtype': subtype,
                    'date': event_date,
                }, report, upsert=True)
            count += 1
        return count
   
    def get_freebies_report(self, event_id, subtype=None, event_date=None, contacts=None):
        count = 0
        if event_date is None:
            event_date = self.yesterday()

        reports = self.create_freebie_report(event_id, subtype, event_date)
        for report in reports:
            self.freebies_report.update({
                    'id': event_id,
                    'subtype': subtype,
                    'date': event_date,
                }, report, upsert=True)
            count += 1
        return count

    def create_contact_report(self, event_id, subtype, event_date, contacts):
        if not self.is_preprocessed:
            print("Preprocessing for contact report...")
            self.assign_contact_ref()
            print("Preprocessing for contact was done.")

        pipeline = [
            { 
                "$match" : {
                    "type" : {
                        "$in" : [
                            TYPE.login, 
                            TYPE.deposit
                        ]
                    }, 
                    REPORT_CONTACT_REF_FIELD_NAME : event_id,
                    REPORT_CONTACT_REF_SUBTYPE_FIELD_NAME: subtype,
                    REPORT_CONTACT_TIME_DELTA_FIELD_NAME: { "$lt": REPORT_ANALYSIS_TIME_WINDOW }
                }
            }, 
            { 
                "$addFields" : {
                    "date" : {
                        "$dateToString" : {
                            "format" : "%Y-%m-%d", 
                            "date" : "$ts"
                        }
                    }
                }
            }, 
            { 
                "$match" : {
                    "date" : event_date
                }
            }, 
            { 
                "$facet" : {
                    "logins" : [
                        {
                            "$match" : {
                                "type" : TYPE.login
                            }
                        }, 
                        {
                            "$group" : {
                                "_id" : "$player"
                            }
                        }, 
                        {
                            "$group" : {
                                "_id" : {

                                }, 
                                "count" : {
                                    "$sum" : 1.0
                                }
                            }
                        }
                    ], 
                    "deposits" : [
                        {
                            "$match" : {
                                "type" : TYPE.deposit
                            }
                        }, 
                        {
                            "$group" : {
                                "_id" : "$player", 
                                "count" : {
                                    "$sum" : 1.0
                                }, 
                                "sum" : {
                                    "$sum" : "$$CURRENT.value"
                                }, 
                                "avg" : {
                                    "$avg" : "$$CURRENT.value"
                                }
                            }
                        }, 
                        {
                            "$group" : {
                                "_id" : {

                                }, 
                                "depositers" : {
                                    "$sum" : 1.0
                                }, 
                                "total_deposits" : {
                                    "$sum" : "$count"
                                }, 
                                "total_value" : {
                                    "$sum" : "$sum"
                                }, 
                                "avg_value" : {
                                    "$avg" : "$avg"
                                }
                            }
                        }
                    ]
                }
            }, 
            { 
                "$project" : {
                    "logins" : {
                        "$arrayElemAt" : [
                            "$logins", 
                            0.0
                        ]
                    }, 
                    "deposits" : {
                        "$arrayElemAt" : [
                            "$deposits", 
                            0.0
                        ]
                    }
                }
            }, 
            { 
                "$project" : {
                    "_id": 0,
                    "id": event_id,
                    "subtype": subtype,
                    "date": event_date,
                    "logged_in_users" : {
                        "$ifNull" : [
                            "$logins.count", 
                            0.0
                        ]
                    }, 
                    "depositers" : {
                        "$ifNull" : [
                            "$deposits.depositers", 
                            0.0
                        ]
                    }, 
                    "total_deposits" : {
                        "$ifNull" : [
                            "$deposits.total_deposits", 
                            0.0
                        ]
                    }, 
                    "total_value" : {
                        "$ifNull" : [
                            "$deposits.total_value", 
                            0.0
                        ]
                    }, 
                    "avg_value" : {
                        "$ifNull" : [
                            "$deposits.avg_value", 
                            0.0
                        ]
                    }
                }
            },
            {
                "$addFields": {
                    "contacts": contacts,
                }
            }
        ]

        cursor = self.events.aggregate(
            pipeline, allowDiskUse = True
        )
        return [doc for doc in cursor]

    def create_freebie_report(self, event_id, subtype, event_date):
        pipeline = [
            { 
                "$match" : {
                    "type" : TYPE.freebie, 
                    "subtype" : subtype, 
                    "id" : event_id,
                }
            }, 
            { 
                "$addFields" : {
                    "date" : {
                        "$dateToString" : {
                            "format" : "%Y-%m-%d", 
                            "date" : "$ts",
                        }
                    }
                }
            }, 
            { 
                "$match" : {
                    "date" : event_date
                }
            }, 
            { 
                "$group" : {
                    "_id" : "$player", 
                    "event" : {
                        "$push" : "$$CURRENT"
                    }, 
                    "count" : {
                        "$sum" : 1.0
                    }
                }
            }, 
            { 
                "$group" : {
                    "_id" : None, 
                    "player" : {
                        "$sum" : 1.0
                    }, 
                    "count" : {
                        "$sum" : "$count"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "id": event_id,
                    "subtype": subtype,
                    "date": event_date,
                    "player": 1,
                    "count": 1,
                }
            }
        ]

        cursor = self.events.aggregate(
            pipeline, allowDiskUse = True
        )
        return [doc for doc in cursor]