import os
import pymysql.cursors
from pymysql.err import MySQLError
import sys
from tabulate import tabulate
from datetime import datetime


def kill_long_running(threshold: int, conn_details: tuple) -> tuple:
    """
    kills any sleeping connections older than the threshold
        :param threshold:int: the threshold for killing connections
        :param conn_details:tuple: a tuple in the structure of host, user, password, schema and charset (optional)
        :return tuple: a tuple containing the killed connections
    """
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

    kill_list = []
    with con.cursor() as cur:
        sql = 'SHOW FULL PROCESSLIST;'
        cur.execute(sql)
        proclist = cur.fetchall()
        for proc in proclist:
            if proc['Command'] == 'Sleep' and proc['Time'] >= threshold:
                kill_list.append(
                    {'id': proc['Id'], 'host': proc['Host'], 'user': proc['User'], 'time': proc['Time']})
                sql = f'KILL {proc["Id"]};'
                cur.execute(sql)

    con.close()
    return tuple(kill_list if len(kill_list) > 0 else [{'message': 'no kills'}])


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
    run_id = datetime.utcnow().isoformat()
    kill_list = kill_long_running(
        os.getenv('MYSSK_TIME_THRESHOLD') or 200, conn_details)
    tbl = tabulate(kill_list, headers='keys')
    print(run_id + '\n' + tbl)
    if not os.path.exists('./runlogs'):
        try:
            os.mkdir('./runlogs')
        except os.error as e:
            print(e)
            exit(1)
    os.chdir('./runlogs')
    with open(f'{run_id}.txt', 'w+') as f:
        f.write(run_id + '\n' + tbl)
