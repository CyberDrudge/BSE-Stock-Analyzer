from bs4 import BeautifulSoup
import csv
import io
import requests
import zipfile

from connect import connect_redis

URL = "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx"


class BhavCopyFetch:

    def extract_zip(self, storage='archives'):
        try:
            req = requests.get(URL)
            soup = BeautifulSoup(req.text, features="html.parser")
            soup_tag = soup.find(id="ContentPlaceHolder1_btnhylZip")
            url = soup_tag['href']
            zipped = requests.get(url)
        except requests.exceptions.HTTPError:
            raise Exception(" HTTP error, Kindly try again.")
        except requests.exceptions.ConnectionError:
            raise Exception("Connection error, Kindly check internet connection.")
        except requests.exceptions.Timeout:
            raise Exception("Request timeout, Kindly check internet connection.")
        except Exception as err:
            raise Exception(err)

        zip_file = zipfile.ZipFile(io.BytesIO(zipped.content))
        zip_file.extractall(storage)
        csv_file_name = zip_file.namelist()[0]
        csv_path = str(storage + '/' + csv_file_name)
        return csv_path, csv_file_name

    def load_zip_to_redis(self):
        redis_conn = connect_redis()
        loaded_data_date = redis_conn.get("latest_date")

        csv_path, csv_file_name = self.extract_zip()
        csv_file_date = csv_file_name[2:4] + "-" + csv_file_name[4:6] + "-20" + csv_file_name[6:8]

        if csv_file_date == loaded_data_date:
            # res["data"] = "Database already have latest data."
            return csv_path, csv_file_name

        csv_list = csv.DictReader(open(csv_path, 'r', encoding="utf-8"))

        redis_pipeline = redis_conn.pipeline(transaction=True)
        redis_pipeline.flushdb()

        for row in csv_list:
            key = row['SC_CODE'].rstrip() + row['SC_NAME'].rstrip()
            value = {'code': row['SC_CODE'],
                     'name': row['SC_NAME'].rstrip(),
                     'open': float(row['OPEN']),
                     'high': float(row['HIGH']),
                     'low': float(row['LOW']),
                     'close': float(row['CLOSE'])}
            change = round(((value['close'] - value['open']) / value['open']) * 100, 2)
            redis_pipeline.hmset(key, value)

            # Assuming logic of top 10 stock entries must be "Highest positive percentage movement first"
            # Using sorted set with percentage as score
            redis_pipeline.zadd("search_sorted", {key: change})

        redis_pipeline.set("latest_date", csv_file_date)
        redis_pipeline.execute()
