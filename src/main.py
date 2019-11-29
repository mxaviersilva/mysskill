import os
import pymysql.cursors
from pymysql.err import MySQLError
import sys


def kill_long_running(threshold: int, conn_details: tuple):
    try:
        con = pymysql.connect(
            host=conn_details[0],
            user=conn_details[1],
            password=conn_details[2],
            db=conn_details[3],
            charset=conn_details[4],
            cursorclass=pymysql.cursors.DictCursor
        )
    except MySQLError as e:
        print(e)
        return
    
    with con.cursor() as cur:
        sql = 'SHOW FULL PROCESSLIST;'
        cur.execute(sql)
        res = cur.fetchall()
        c = 0
        for r in res:
            if r['Command'] == 'Sleep' and r['Time'] >= threshold:
                print(f'killing {r["Id"]}')
                sql = f'KILL {r["Id"]};'
                cur.execute(sql)
                c += 1
        print(f'killed {c} processes')

    con.close()


if __name__ == "__main__":
    if os.getenv('MYSSK_ENV') != 'PROD':
        from dotenv import load_dotenv
        load_dotenv()

    conn_details = (
        os.getenv('DB_HOST'),
        os.getenv('DB_USER'),
        os.getenv('DB_PASSWORD'),
        os.getenv('DB_SCHEMA'),
        os.getenv('DB_CHARSET')
    )

    kill_long_running(os.getenv('MYSSK_TIME_THRESHOLD') or 200, conn_details)
