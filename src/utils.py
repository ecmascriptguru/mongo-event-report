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
        self.reports = self.db[REPORT_RESULTS_TABLE_NAME]
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
                doc[REPORT_CONTACT_REF_FIELD_NAME] = contact_ref['_id']
                doc[REPORT_CONTACT_REF_SUBTYPE_FIELD_NAME] = contact_ref['subtype']
                doc[REPORT_CONTACT_TIME_DELTA_FIELD_NAME] =\
                    int((doc['ts'] - contact_ref['ts']).total_seconds() / (60 * 60))
                self.events.save(doc)
        self.is_preprocessed = True
        return True
    
    def get_possible_report_keys(self, date=None):
        collection = self.events

        if date is not None:
            pipeline = [
                {
                    u"$match": {
                        u"type": u"contact",
                        u"subtype": {
                            u"$nin": [
                                u"notification"
                            ]
                        },
                    }
                }, 
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
                        u"date": date,
                    }
                },
                {
                    u"$group": {
                        u"_id": {
                            u"id": u"$id",
                            u"date": u"$date"
                        }
                    }
                }, 
                {
                    u"$project": {
                        u"_id": 0.0,
                        u"id": u"$_id.id",
                        u"date": u"$_id.date"
                    }
                }
            ]
        else:
            pipeline = [
                {
                    u"$match": {
                        u"type": u"contact",
                        u"subtype": {
                            u"$nin": [
                                u"notification"
                            ]
                        }
                    }
                }, 
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
                    u"$group": {
                        u"_id": {
                            u"id": u"$id",
                            u"date": u"$date"
                        }
                    }
                }, 
                {
                    u"$project": {
                        u"_id": 0.0,
                        u"id": u"$_id.id",
                        u"date": u"$_id.date"
                    }
                }
            ]

        cursor = collection.aggregate(
            pipeline, 
            allowDiskUse = True
        )
        return [(doc['id'], doc['date']) for doc in cursor]
    
    def report_for_all_data(self, date=None):
        count = 0
        
        if not self.is_preprocessed:
            self.assign_contact_ref()

        pipeline = [
            {
                u"$match": {
                    u"type": u"contact",
                    u"subtype": {
                        u"$nin": [
                            u"notification"
                        ]
                    },
                }
            }, 
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
        ]

        if date is not None:
            pipeline.append(
                {
                    u"$match": {
                        u"date": date,
                    }
                }
            )

        pipeline += [
            {
                u"$group": {
                    u"_id": {
                        u"id": u"$id",
                        u"subtype": u"$subtype",
                        u"date": u"$date",
                    },
                    u"_ids": {
                        u"$push": u"$$ROOT._id"
                    }
                }
            }, 
            {
                u"$lookup": {
                    u"from": EVENTS_TABLE_NAME,
                    u"let": {
                        u"event_ids": u"$_ids"
                    },
                    u"pipeline": [
                        {
                            u"$match": {
                                u"$and": [
                                    {
                                        u"$expr": {
                                            u"$in": [
                                                u"$$CURRENT.%s" % REPORT_CONTACT_REF_FIELD_NAME,
                                                u"$$event_ids"
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
                                                u"$$CURRENT.%s" % REPORT_CONTACT_TIME_DELTA_FIELD_NAME,
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
                                    u"$sum": 1
                                }
                            }
                        }
                    ],
                    u"as": u"logins"
                }
            }, 
            {
                u"$lookup": {
                    u"from": EVENTS_TABLE_NAME,
                    u"let": {
                        u"event_ids": u"$_ids"
                    },
                    u"pipeline": [
                        {
                            u"$match": {
                                u"$and": [
                                    {
                                        u"$expr": {
                                            u"$in": [
                                                u"$$CURRENT.%s" % REPORT_CONTACT_REF_FIELD_NAME,
                                                u"$$event_ids"
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
                                                u"$$CURRENT.%s" % REPORT_CONTACT_TIME_DELTA_FIELD_NAME,
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
                                    u"$sum": 1
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
                    u"id": u"$_id.id",
                    u"subtype": u"$_id.subtype",
                    u"date": u"$_id.date",
                    u"contacts": {
                        u"$size": u"$_ids"
                    },
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
        for report in cursor:
            self.reports.update({
                    u"id": report["id"],
                    u"subtype": report["subtype"],
                    u"date": report["date"]
                }, report, upsert=True)
            count += 1
        return count

    def get_report(self, event_id, date=None):
        count = 0
        if date is None:
            yesterday = date.today() - timedelta(1)
            date = yesterday.strftime('%Y-%m-%d')

        reports = self.create_reports(event_id, date)
        for report in reports:
            self.reports.update({
                    u"id": report["id"],
                    u"subtype": report["subtype"],
                    u"date": report["date"]
                }, report, upsert=True)
            count += 1
        return count
            

    def create_reports(self, event_id, date):
        if not self.is_preprocessed:
            self.assign_contact_ref()

        pipeline = [
            {
                u"$match": {
                    u"type": u"contact",
                    u"subtype": {
                        u"$nin": [
                            u"notification"
                        ]
                    },
                    u"id": event_id,
                }
            }, 
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
                    u"date": date,
                }
            },
            {
                u"$group": {
                    u"_id": {
                        u"id": u"$id",
                        u"subtype": u"$subtype",
                        u"date": u"$date",
                    },
                    u"_ids": {
                        u"$push": u"$$ROOT._id"
                    }
                }
            }, 
            {
                u"$lookup": {
                    u"from": EVENTS_TABLE_NAME,
                    u"let": {
                        u"event_ids": u"$_ids"
                    },
                    u"pipeline": [
                        {
                            u"$match": {
                                u"$and": [
                                    {
                                        u"$expr": {
                                            u"$in": [
                                                u"$$CURRENT.%s" % REPORT_CONTACT_REF_FIELD_NAME,
                                                u"$$event_ids"
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
                                                u"$$CURRENT.%s" % REPORT_ANALYSIS_TIME_WINDOW,
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
                                    u"$sum": 1
                                }
                            }
                        }
                    ],
                    u"as": u"logins"
                }
            }, 
            {
                u"$lookup": {
                    u"from": EVENTS_TABLE_NAME,
                    u"let": {
                        u"event_ids": u"$_ids"
                    },
                    u"pipeline": [
                        {
                            u"$match": {
                                u"$and": [
                                    {
                                        u"$expr": {
                                            u"$in": [
                                                u"$$CURRENT.%s" % REPORT_CONTACT_REF_FIELD_NAME,
                                                u"$$event_ids"
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
                                                u"$$CURRENT.%s" % REPORT_ANALYSIS_TIME_WINDOW,
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
                                    u"$sum": 1
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
                    u"id": u"$_id.id",
                    u"subtype": u"$_id.subtype",
                    u"date": u"$_id.date",
                    u"contacts": {
                        u"$size": u"$_ids"
                    },
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