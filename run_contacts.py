import sys
from datetime import date, timedelta, datetime
from src.config import ENV, COMMAND_OPTIONS, COMMAND_DATE_OPTIONS
from src.utils import DataDog


def run(args):
    if len(args) < 2:
        raise Exception("""Missing arguments.
            You should give 2 args least.
            Options
            --all: report for all existing events data
            --date: report for events data for the given date

            Possible usages
            $ python run_contacts.py --all
            $ python run_contacts.py --date 2018-12-31
            $ python run_contacts.py welcome-offer-reminder 2018-12-31
            """
        )

    command_option = args[1]
    count = 0

    dog = DataDog()
    if command_option == COMMAND_OPTIONS.preprocess:
        dog.assign_contact_ref()
    elif command_option == COMMAND_OPTIONS.all:
        print("Script is running with %s option in %s mode..." % (COMMAND_OPTIONS.all, ENV))
        count = dog.report_for_all_data()
    elif command_option == COMMAND_OPTIONS.date:
        if len(args) < 3 or args[2] == COMMAND_DATE_OPTIONS.yesterday:
            date_string = DataDog.yesterday()
        elif args[2] == COMMAND_DATE_OPTIONS.today:
            date_string = DataDog.today()
        else:
            date_string = args[2]

        print("Script is running with %s option for %s in %s mode..." % (COMMAND_OPTIONS.date, date_string, ENV))
        count = dog.report_for_all_data(date_string)
    else:
        event_id = args[1]
        if len(args) > 3:
            subtype = args[2]
            date_string = args[3]
        else:
            yesterday = date.today() - timedelta(1)
            date_string = yesterday.strftime('%Y-%m-%d')
        
        print("Script is running with event %s(%s) at (%s) in %s mode..." % (event_id, subtype, date_string, ENV))
        count = dog.get_report(event_id, subtype, date_string)

    print("%d report(s) created in %s mode." % (count, ENV))

if __name__ == "__main__":
    start_point = datetime.now()
    run(sys.argv)
    end_point = datetime.now()
    execusion_time = end_point - start_point
    print("It took %d seconds to be finished." % execusion_time.seconds)