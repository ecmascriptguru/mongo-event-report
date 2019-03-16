import sys
from datetime import date, timedelta
from src.config import ENV, COMMAND_OPTIONS
from src.utils import DataDog


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("""Missing arguments.
            You should give 2 args least.
            Options
            --all: report for all existing events data
            --date: report for events data for the given date

            Possible usages
            $ python run.py --all
            $ python run.py --date 2018-12-31
            $ python run.py welcome-offer-reminder 2018-12-31
            """
        )

    command_option = sys.argv[1]
    count = 0

    dog = DataDog()
    if command_option == COMMAND_OPTIONS.all:
        print("Script is running with %s option in %s mode..." % (COMMAND_OPTIONS.all, ENV))
        count = dog.report_for_all_data()
    elif command_option == COMMAND_OPTIONS.date:
        if len(sys.argv) < 3:
            raise Exception("Date option is missing.")
        date = sys.argv[2]

        print("Script is running with %s option for %s in %s mode..." % (COMMAND_OPTIONS.all, date, ENV))
        count = dog.report_for_all_data(date)
    else:
        event_id = sys.argv[1]
        if len(sys.argv) > 2:
            date = sys.argv[2]
        else:
            yesterday = date.today() - timedelta(1)
            date = yesterday.strftime('%Y-%m-%d')
        
        print("Script is running with event(%s) at (%s) in %s mode..." % (event_id, date, ENV))
        count = dog.get_report(event_id, date)

    print("%d report(s) created in %s mode." % (count, ENV))
