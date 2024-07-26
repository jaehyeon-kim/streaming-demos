import os
import psycopg2
import psycopg2.extras


class DbHelper:
    def __init__(self) -> None:
        self.conn = self.connect_db()

    def connect_db(self):
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "devdb"),
            user=os.getenv("DB_USER", "devuser"),
            password=os.getenv("DB_PASSWORD", "password"),
        )
        conn.autocommit = False
        return conn

    def get_connection(self):
        if (self.conn is None) or (self.conn.closed):
            self.conn = self.connect_db()

    def fetch_records(self, stmt: str):
        self.get_connection()
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(stmt)
            return cur.fetchall()

    def update_records(self, stmt: str, records: list, to_fetch: bool = True):
        self.get_connection()
        with self.conn.cursor() as cur:
            values = psycopg2.extras.execute_values(cur, stmt, records, fetch=to_fetch)
            self.conn.commit()
            if to_fetch:
                return values

    def commit(self):
        if not self.conn.closed:
            self.conn.commit()

    def close(self):
        if self.conn and (not self.conn.closed):
            self.conn.close()
