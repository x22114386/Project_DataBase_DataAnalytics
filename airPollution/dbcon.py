import traceback
import sys
import pymysql
import paramiko
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine, schema



def mySqlConn():
    hostname = '127.0.0.1'
    username = 'root'
    password = 'root'
    port = 3306
    database = 'X22114386Project'

    try:
        conn = pymysql.connect(host=hostname, user=username, passwd=password, port=port, database=database)
        return conn
    
    except Exception as e:
        print("Error while creating mysql connection")
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(f"Caught exception {exc_type} with message {e}")
        print("Traceback:")
        traceback.print_tb(exc_traceback)



def mySqlEngine():
    hostname = '127.0.0.1'
    username = 'root'
    password = 'root'
    port = 3306
    database = 'X22114386Project'

    # Configuration
    ssh_host = '87.44.4.70'
    ssh_user = 'debian'
    ssh_port = 22


    try:
        ssh_key_path = "C:/Users/Manu/Downloads/StudiesNCI/MichealBrad_DB/projects/DataBase_And_Analytics_Project/new.pem"
        mypkey = paramiko.RSAKey.from_private_key_file(ssh_key_path)
    
        tunnel = SSHTunnelForwarder((ssh_host, ssh_port), ssh_pkey=mypkey, ssh_username=ssh_user, remote_bind_address=(hostname, 3306)) 
        tunnel.start()
        port=tunnel.local_bind_port
        engine = create_engine(f'mysql+pymysql://{username}:{password}@{hostname}:{port}/{database}')
        return engine
    
    except Exception as e:
        print("Error while creating mysql engine")
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(f"Caught exception {exc_type} with message {e}")
        print("Traceback:")
        traceback.print_tb(exc_traceback)
        



def postgresEngine():
    hostname = '87.44.4.70'
    username = 'dap'
    password = 'dap'
    port = 5432
    database = 'X22114386Project'

    try:
        engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/postgres')
        
        # if we need new db, will use this
        # with engine.connect() as conn:
            
        #     #Creating database if not exits
        #     result = conn.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = '"+ database +"'")
        #     exists = result.fetchone()
        #     if not exists:
        #         conn.execute("commit")
        #         conn.execute("CREATE DATABASE "+ database)
        #         print("Database created successfully........")
        #         conn.close()
        # Point engine to right DB
        # engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database}')    
        
        
        # schema creation
        # if not engine.dialect.has_schema(engine, database):
        #     engine.execute(schema.CreateSchema(database))
        
        
        return engine
    
    except Exception as e:
        print("Error while creating postgres engine")    
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(f"Caught exception {exc_type} with message {e}")
        print("Traceback:")
        traceback.print_tb(exc_traceback)