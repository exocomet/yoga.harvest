import requests
import json
import psycopg2
import sshtunnel


DB_USER = 'dbtest'
DB_PASS = 'password'


URL = 'https://api.pro.coinbase.com'

def get_candles(p_id):
    r = requests.get('{url}/products/{product_id}/candles'.format(url=URL, product_id=p_id),
        params={'start': '2019-05-10', 'end': '2019-05-14', 'granularity': 86400}
    )
    candles = r.json()
    return candles


def db_query(sql, params):
    params = {
        'user': DB_USER,
        'password': DB_PASS,
        'host': '127.0.0.1',
        'port': 5432,
        'database': 'dbname',
    }
    conn = psycopg2.connect(**params)
    curs = conn.cursor()

    curs.execute(sql)
    r = curs.fetchall()
    #curs.commit()
    return r


def get_insert_statement():
    cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'granularity']
    sql = """
INSERT INTO public.ohlc_coinbase({cs})
VALUES ({ps});
    """.format(cs=cols, ps=', '.join(len(cols)*['%s']))
    return sql


if __name__ == '__main__':
    #p_id = 'BTC-EUR'
    data = get_candles('BTC-EUR')
    sql = get_insert_statement()
    for tick in candles:
        db_query(sql, tick)
