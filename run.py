from src.config import ENV
from src.utils import DataDog


if __name__ == "__main__":
    print("Script is running in %s mode..." % ENV)
    dog = DataDog()
    print(dog.get_report())