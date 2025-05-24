import multiprocessing as mp
import psycopg2
import time

from ..constants import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_USER

LOG = mp.get_logger()


class PostGisManager:
    def __init__(self):
        pass

    def connect():
        """
        Wait until PostGIS is running
        """

        global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

        LOG.info("Attempting connection to PostGIS...")

        while True:
            try:
                conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    dbname=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                )
                cur = conn.cursor()
                cur.close()
                break
            except:
                time.sleep(5)

        LOG.info("Connection to PostGIS successful")
