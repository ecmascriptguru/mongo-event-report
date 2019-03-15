import sys
from src.config import ENV
from src.utils import DataDog


if __name__ == "__main__":
    event_ids = []
    if len(sys.argv) > 1:
        event_ids = sys.argv[1:]

    print("Script is running in %s mode..." % ENV)
    dog = DataDog()    
    
    cursor = dog.get_report(event_ids)
    for doc in cursor:
        print(doc)