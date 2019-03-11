import os, pandas
from pymongo import MongoClient
from src.config import ENV



if __name__ == "__main__":
    print("Script is running in %s mode..." % ENV)