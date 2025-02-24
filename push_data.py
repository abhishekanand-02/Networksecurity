import os
import sys
import json
import certifi
import pandas as pd
import pymongo
from dotenv import load_dotenv
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
ca = certifi.where()

class NetworkDataExtract:
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def csv_to_json_convertor(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def insert_data_mongodb(self, records, database, collection):
        try:
            mongo_client = pymongo.MongoClient(MONGO_DB_URL, tlsCAFile=ca)
            db = mongo_client[database]
            collection = db[collection]
            collection.insert_many(records)
            return len(records)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

if __name__ == '__main__':
    FILE_PATH = "/home/abhishek.anand/Desktop/Bootcamp_MLops/Networksecurity/Network_Data/phisingData.csv"
    DATABASE = "ABHISHEKAI"
    COLLECTION = "NetworkData"

    try:
        network_obj = NetworkDataExtract()
        records = network_obj.csv_to_json_convertor(file_path=FILE_PATH)
        no_of_records = network_obj.insert_data_mongodb(records, DATABASE, COLLECTION)
        print(f"Successfully inserted {no_of_records} records into the database '{DATABASE}', collection '{COLLECTION}'.")
    except Exception as e:
        print(f"An error occurred: {e}")
