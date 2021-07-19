import pandas as pd
from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine, UniqueConstraint
from sqlalchemy.engine.reflection import Inspector


class AbstractClient:
    def __init__(self, connection_string=""):
        self._client = None
        self.connection_string = connection_string

    @classmethod
    def connect(cls, connection_string):
        raise NotImplemented

    def disconnect(self):
        raise NotImplemented

    def write_data(self, data: pd.DataFrame) -> bool:
        raise NotImplemented


class CSVClient(AbstractClient):
    default_connection_string = "output.csv"

    def __init__(self, connection_string=default_connection_string):
        super(CSVClient, self).__init__(connection_string)

    def write_data(self, data: pd.DataFrame) -> bool:
        data.to_csv(self.connection_string, mode="a", index=False)


class SQLiteClient(AbstractClient):
    """
    SQLite client, based on an AbstractClient class to inherit method names.
    So you can change the database format without changing the core code
    """
    default_connection_string = "output.db"

    def __init__(self, connection_string=default_connection_string):
        super().__init__(connection_string)
        self.connect(connection_string)

    def connect(self, connection_string):
        self._client = create_engine(f'sqlite:///{connection_string}')
        self._connection = self._client.connect()
        inspector = Inspector.from_engine(self._client)
        if "Data" not in inspector.get_table_names():
            self.create_data_table()
        else:
            print("DB File already exists")

    def create_data_table(self, columns=[]):
        """
        Create the data table. We should find a way to generify this part. The pandas way to create the database seems
        to use wrong schema. So data was not accessible
        :param columns:
        :return:
        """
        columns = [("id", String, True),
                   ("year", Integer, False),
                   ("month", Integer, False),
                   ("day", Integer, False),
                   ("email", String, False),
                   ("uri", String, False),
                   ("action", String, False),
                   ("tags", String, False)
                   ]
        metadata = MetaData(bind=self._client)
        output = Table('Data', metadata,
                       *(Column(c[0], c[1],
                                primary_key=c[2])
                         for c in columns),
                       UniqueConstraint('id', sqlite_on_conflict='IGNORE')
                       )
        output.create()

    def disconnect(self):
        self._client.close()

    def write_data(self, data: pd.DataFrame) -> bool:
        data.to_sql('Data', con=self._client, if_exists="append", index=False)
