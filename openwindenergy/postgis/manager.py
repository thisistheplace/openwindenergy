from contextlib import contextmanager
import multiprocessing as mp
import psycopg2
from psycopg2.extensions import AsIs
import time

from ..constants import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_USER

LOG = mp.get_logger()


class Db:
    def __init__(self):
        pass

    @contextmanager
    def connection(self):
        """
        Wait until PostGIS is running
        """
        global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
        while True:
            try:
                conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    dbname=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=5,
                    keepalives_count=5,
                )
                yield conn
                break
            except:
                time.sleep(5)
        conn.close()

    @contextmanager
    def cursor(self):
        with self.connection() as conn:
            cur = self.connection().cursor()
            yield conn, cur
            cur.close()

    def exec(self, sql_text, sql_parameters):
        """
        Executes SQL statement
        """
        with self.cursor() as [conn, cur]:
            cur.execute(sql_text, sql_parameters)
            conn.commit()

    def drop_table(self, table_name):
        """
        Drops PostGIS table
        """
        self.exec("DROP TABLE IF EXISTS %s", (AsIs(table_name),))

    def table_exists(self, table_name) -> bool:
        """
        Checks whether table already exists
        """
        table_name = table_name.replace("-", "_")
        with self.cursor() as [conn, cur]:
            cur.execute(
                "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s);",
                (table_name,),
            )
            tableexists = cur.fetchone()[0]
        return tableexists

    def fetch_all(self, sql_text, sql_parameters):
        """
        Runs database query and returns results
        """
        with self.cursor() as [conn, cur]:
            cur.execute(sql_text, sql_parameters)
            return cur.fetchall()

    def get_table_bounds(self, table_name):
        """
        Get bounds of all geometries in table
        """
        with self.cursor() as [conn, cur]:
            cur.execute(
                """
            SELECT 
                MIN(ST_XMin(geom)) AS left,
                MIN(ST_YMin(geom)) AS bottom,
                MAX(ST_XMax(geom)) AS right,
                MAX(ST_YMax(geom)) AS top FROM %s;
            """,
                (AsIs(table_name),),
            )
            left, bottom, right, top = cur.fetchone()
        return {"left": left, "bottom": bottom, "right": right, "top": top}
