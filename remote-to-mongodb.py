import subprocess
from pymongo import MongoClient , errors
import pandas as pd


def download_csv(container_id,path):
    # Docker CLI komutunu oluşturun
    docker_command = ["docker", "cp", f"{container_id}:{path}", "./data.csv"]

    # Docker CLI komutunu çalıştırın
    subprocess.run(docker_command)

def upload_to_mongodb():
    try:
        client=MongoClient("mongodb://localhost:27017/",
                                         username='user',
                                         password='pass',
                                         authSource="admin",
                                         authMechanism="SCRAM-SHA-256")

        db = client['my-database']
        collection = db['my-collection']


        # CSV dosyasını pandas DataFrame'e yükleyin
        df = pd.read_csv('./data.csv')

        # DataFrame'deki verileri MongoDB'ye yazın
        records = df.to_dict(orient='records')
        collection.insert_many(records)

    except errors.OperationFailure as e:
        print(f"Hata: {e} ")
    except errors.ConnectionFailure as e:
        print(f"Hata: {e}")

if __name__ == '__main__':
    
    download_csv(container_id='ed481246713f',path='/data/car_prices.csv')

    upload_to_mongodb()