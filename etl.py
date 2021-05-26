import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    loading data from s3 to staging tables using list taht contain queires defined in sql_queries.py 
    
    Keyword arguments:
    
    cur -- Cursor object to perform SQL commands using execute method
    conn -- Connection object that represents the connecting to database
    """
        
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    transforming  data from staging tables dwh tables using queires defined in sql_queries.py 
    
    Keyword arguments:
    
    cur -- Cursor object to perform SQL commands using execute method
    conn -- Connection object that represents the connecting to database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    main method 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()