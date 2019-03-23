import sys
from datetime import datetime
from run_contacts import run as run_contacts_report
from run_freebies import run as run_freebies_report


class REPORT_OPTIONS:
    all = '--all'
    date = '--date'

def run(args):
    run_contacts_report(args)
    run_freebies_report(args)


if __name__ == "__main__":
    start_point = datetime.now()
    run(sys.argv)
    end_point = datetime.now()
    execusion_time = end_point - start_point
    print("It took %d seconds to be finished." % execusion_time.seconds)