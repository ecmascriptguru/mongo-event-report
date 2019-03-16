import sys
from datetime import date, timedelta
from src.config import ENV
from src.utils import DataDog


if __name__ == "__main__":
    event_ids = []
    if len(sys.argv) < 2:
        raise Exception("Missing arguments.\n"
        "You should give event id at least. Date is optional.\n"
        "python run.py welcome-offer-reminder 2018-12-31")

    event_id = sys.argv[1]
    if len(sys.argv) > 2:
        date = sys.argv[2]
    else:
        yesterday = date.today() - timedelta(1)
        date = yesterday.strftime('%Y-%m-%d')

    print("Script is running with event(%s) at (%s) in %s mode..." % (event_id, date, ENV))
    
    dog = DataDog()
    
    print(dog.get_report(event_id, date))