from Client import *
from os import remove
from os.path import isfile
import argparse
from boto3 import session
import pandas as pd
import time


MODE_LOAD = "load"
MODE_COMPUTE = "compute"


class Processor:

    def __init__(self, mode=MODE_LOAD):
        self.mode = mode

    def read_and_load(self, year, month):
        """
        Download the file, then read it with chunks. Files are stored with format {year}/{month}/events.csv
        """
        start = time.time()
        self.download_file(year, month)
        print(f"Download time : {time.time() - start } sec")
        self.load_file_content(chunksize=100000)
        print(f"Total time : {time.time() - start} sec")

    def download_file(self, year, month):
        # Need something to manage already downloaded files (in order to reduce operations)
        print("Download file for period", f"{str(month).zfill(2)}/{year}")
        _session = session.Session()
        _s3 = _session.client("s3")
        _s3.download_file(Bucket="work-sample-mk", Key=f"{year}/{str(month).zfill(2)}/events.csv", Filename="events.csv")

    def clean_data(self, rows: pd.DataFrame) -> pd.DataFrame:
        """
        Clean data to have a database-ready format (note : here is a sample for POC,
            we may have some parameters to manage the expected format
        :param rows: input dataframe (format of input files)
        :return: a Dataframe with expected format
        """
        # Explode timestamp
        print("Cleaning the chunk")
        rows = pd.concat([pd.DataFrame(rows.timestamp.str.split('-').tolist(), columns=['year', 'month', 'day'],
                                       index=rows.index), rows], axis=1, join="inner")

        # Remove some columns (id and timestamp)
        # rows = rows.drop(labels=['timestamp', 'id'], axis=1)
        rows = rows.drop(labels=['timestamp'], axis=1)
        # Assuming we store user personal data elsewhere (ie: country and ip)
        rows = rows.drop(labels=['country', 'ip'], axis=1)

        # Split the "tag" column into multiple lines
        # rows.tags = rows.tags.apply(eval)
        # rows = rows.explode('tags')
        return rows

    def connect_client(self) -> AbstractClient:
        if not self.client:
            self.client = SQLiteClient()
        return self.client

    def load_file_content(self, chunksize=10000):
        """
        Use pandas chunk read for better memory performances
        Clean data for each chunk
        Load each chunk in database
        :param chunksize: To be adapted for performance optimization
        :return:
        """
        chunks = pd.read_csv("events.csv", chunksize=chunksize, header=0)
        client = self.connect_client()
        for chunk in chunks:
            start = time.time()
            chunk = self.clean_data(chunk)
            client.write_data(chunk)
            print(f"Chunk time : {time.time() - start} sec")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some large files')
    parser.add_argument('--mode', type=str, required=True,
                        help='process mode : read input file and "load" in DB, or "compute" data',
                        choices=['load', 'compute'])
    parser.add_argument('--year', type=int, nargs='?', default=2021,
                        help='year folder to retrieve data')
    parser.add_argument('--month', type=int, nargs='?', default=4,
                        help='month folder to retrieve data')

    args = parser.parse_args()
    p = Processor(args.mode)
    if args.mode == MODE_LOAD:
        p.read_and_load(args.year, args.month)
    elif args.mode == MODE_COMPUTE:
        print("Compute mode not implemented yet")
