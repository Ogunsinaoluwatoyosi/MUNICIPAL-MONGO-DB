from pymongo import MongoClient
import csv


def ingest_from_csv(filename, collection_name):
    try:
            with open(filename, newline='') as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                entry = {}
                for idx, row in enumerate(reader):
                    if idx == 0:
                        continue
                    try:
                        entry = {
			    "Country": row[1],
                            "Total population served by municipal waste collection(%)" : row[2],

                        }

                        collection_name.insert_one(entry)

                    except:
                        return


def get_database():
    CONNECTION_STRING = "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
    client = MongoClient(CONNECTION_STRING)
    return client['mydata']


if __name__ == "__main__":    
    dbname = get_database()
    collection_name = dbname['gpi_primary']
    ingest_from_csv('Municipal \ 2011.csv', collection_name)