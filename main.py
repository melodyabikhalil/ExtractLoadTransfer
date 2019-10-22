import psycopg2
import json
from pprint import pprint


def db_connection(user,password,host,port,database):
    try:
        connection = psycopg2.connect(user = user,
                                      password = password,
                                      host = host,
                                      port = port,
                                      database = database)

        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print ( connection.get_dsn_parameters(),"\n")

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record,"\n")
        return connection
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)

def db_close(connection):
    connection.cursor().close()
    connection.close()
    print("PostgreSQL connection is closed")

def select_query_timestamp(source_conx, source_table, timestamp_name, timestamp_value, now):
    cursor = source_conx.cursor()
    select_query = "SELECT * FROM "+source_table+" WHERE "+timestamp_name+"  > "+timestamp_value"+  and "+timestamp_name+" <= "+now
    records = cursor.fetchAll()
    return records

def select_query_pk(source_conx, source_table, pk):
    cursor = source_conx.cursor()
    select_query = "SELECT * FROM "+source_table+" WHERE "+pk+" = "+pk
    records = cursor.fetchAll()
    return records

def insert_update_query(record, target_conx, target_table, pk):
    cursor = target_conx.cursor()
    select_query = "SELECT * FROM "+target_table+" WHERE "+pk+" = "+record[0]
    target_result = cursor.fetchAll()
    query = "" 
    if len(target_result) >0 :
        query = "UPDATE "+target_table+" SET "+"WHERE "+pk+" = "+record[0]
    else :
        query = "INSERT "
    cursor.execute(query)
    

def open_file(file_name):
    with open(file_name) as f:
        data = json.load(f)
    pprint(data)

def get_current_datetime(source_conx):
    cursor = source_conx.cursor()
    cursor.execute("SELECT now()")
    now = cursor.fetchAll()
    return now

def main():
    database_info = open_file("database_info.json")
    last_runtimes = open_file("last_runtimes.json")
    
    source_conx = db_connection()
    target_conx = db_connection()

    source_cursor = source_conx.cursor()
    target_cursor = target_conx.cursor()

    for table in database_info["tables"]:
        now = get_current_datetime(source_conx)
        table_name = table["name"]
        records = select_query_timestamp(source_conx, table_name, table["timestamp"], last_runtimes[table_name], now)
        for record in records:
            insert_update_query(record, target_conx, table['target_table'], table["pk"])
        for child in table["children"]:
            child_records = select_query_pk(source_conx, child["name"], child["pk"])
            for child_record in child_records:
                insert_update_query(child_record, target_conx, child['target_table'], child["pk"])
        last_runtimes[table_name] = now
    
    db_close(source_conx)
    db_close(target_conx)
        
    
