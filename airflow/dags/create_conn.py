import logging
from airflow import settings
from airflow.models import Connection

def create_conn(conn_id, conn_type, host, login, pwd, port, desc):
    conn = Connection(conn_id=conn_id,
                      conn_type=conn_type,
                      host=host,
                      login=login,
                      password=pwd,
                      port=port,
                      description=desc)
    session = settings.Session()
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

    if str(conn_name) == str(conn.conn_id):
        logging.warning(f"Connection {conn.conn_id} already exists")
        return

    session.add(conn)
    session.commit()
    logging.info(Connection.log_info(conn))
    logging.info(f'Connection {conn_id} is created')
    
def create_essential_conn():
    create_conn("spark_conn", "spark", "spark://spark-master", "", "", "7077", "")
    create_conn("hive_conn", "hiveserver2", "hive-server2", "", "", "10000", "") 