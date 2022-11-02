import pandas as pd
import psycopg2


class DataLoader:
    db = None
    user = None

    def __init__(self, db=None, user=None):
        self.db = db
        self.user = user

    def get_db_connection(self):
        return psycopg2.connect(f"dbname={self.db} user={self.user}")
        pass

    @staticmethod
    def read_csv(file_path: str, *args, **kwargs) -> pd.DataFrame:
        df = pd.read_csv(file_path, *args, **kwargs)
        return df

    def select_from_db(self, table: str) -> pd.DataFrame:
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'select * from {table}')
                df = pd.Dataframe(cur.fetchall())

        return df
