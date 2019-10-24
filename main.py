import psycopg2
import json
from pprint import pprint

global json_database_info
global json_last_runtimes

def db_connection(user,password,host,port,database):
    try:
        connection = psycopg2.connect(user = user,
                                      password = password,
                                      host = host,
                                      port = port,
                                      database = database)

        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        return connection
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)

def db_close(connection):
    connection.cursor().close()
    connection.close()
    print("PostgreSQL connection is closed")

def get_columns(source_conx, table_name):
    cursor = source_conx.cursor()
    select_query = "SELECT * FROM "+table_name+" WHERE 1=1"
    cursor.execute(select_query)
    columns = [i[0] for i in cursor.description]
    return columns

def select_query_timestamp(source_conx, source_table, timestamp_name, timestamp_value, now):
    cursor = source_conx.cursor()
    select_query = "SELECT * FROM "+source_table+" WHERE "+timestamp_name+"  > '"+timestamp_value+"'  and "+timestamp_name+" <= '"+now+"'"
    cursor.execute(select_query)
    records = cursor.fetchall()
    return records

def select_query_pk(source_conx, source_table, child_fk, parent_record):
    cursor = source_conx.cursor()
    select_query = "SELECT * FROM "+source_table+" WHERE "+child_fk+" = '"+str(parent_record[0])+"'"
    cursor.execute(select_query)
    if (cursor.rowcount <= 0):
        records = []
    else:
        records = cursor.fetchall()
    return records

def insert_update_query(record, target_conx, target_table, pk, columns):
    cursor = target_conx.cursor()
    select_query = "SELECT * FROM "+target_table+" WHERE "+pk+" = '"+str(record[0])+"'"
    query = ""
    cursor.execute(select_query)
    if (cursor.rowcount <= 0):
        query = prepare_insert_query(target_table, record, columns)
    else:
        query = prepare_update_query(target_table, record, pk, record[0], columns)
    cursor.execute(query)
    target_conx.commit()
    
def prepare_insert_query(table_name, record, columns):
    query = "INSERT INTO " + table_name+ " ("
    for i in columns:
        query += str(i)+","
    query = query[:len(query)-1]
    query += ") VALUES ("
    for i in record:
        value = str(i)
        if (value == 'None'):
            query +="NULL,"
        else:
            j = 0
            while j<len(value):
                if value[j]=="'":
                    value = value[:j] + "'" +value[j:]
                    j+=2
                else:
                    j+=1
            query += "'"+str(value)+"',"
    query = query[:len(query)-1]
    query += ")"
    return query
    
def prepare_update_query(table_name, record, pk_name, pk_value, columns):
    query = "UPDATE " + table_name+ " SET "
    i = 0
    while (i<len(columns)):
        value = str(record[i])
        if (value == 'None'):
            query+= str(columns[i])+" = NULL,"
        else:
            j = 0
            while j<len(value):
                if value[j]=="'":
                    value = value[:j] + "'" +value[j:]
                    j+=2
                else:
                    j+=1
            query+= str(columns[i])+" = '"+str(value)+"' ,"
        i+=1
    query=query[:len(query)-1] 
    query +=" WHERE "+pk_name+" = '"+str(pk_value)+"'"
    return query
    
def open_file(file_name):
    with open(file_name) as f:
        data = json.load(f)
    return data

def write_file(file_name, data):
    with open(file_name, 'w') as outfile:
        json.dump(data, outfile)

def get_current_datetime(source_conx):
    cursor = source_conx.cursor()
    cursor.execute("SELECT now()")
    now = cursor.fetchall()
    return str(now[0][0])

def open_save_json(database_info, last_runtimes):
    global json_database_info
    global json_last_runtimes
    json_database_info = open_file(database_info)
    json_last_runtimes = open_file(last_runtimes)

def main():
    global json_database_info
    global json_last_runtimes
    open_save_json("./database_info.json","./last_runtimes.json")
    user = "postgres"
    password = "P@ssw0rd"
    port = 5432
    source_conx = db_connection(user, password, json_database_info["source_server"], port, json_database_info["source_schema"])
    target_conx = db_connection(user, password, json_database_info["target_server"], port, json_database_info["target_schema"])

    source_cursor = source_conx.cursor()
    target_cursor = target_conx.cursor()

    for table in json_database_info["tables"]:
        now = get_current_datetime(source_conx)
        table_name = table["name"]
        records = select_query_timestamp(source_conx, table_name, table["timestamp"], json_last_runtimes[table_name], now) 
        columns = get_columns(source_conx, table_name)
        table["columns"] = columns 
        for record in records:
            insert_update_query(record, target_conx, table['target_table'], table["pk"], columns)
            for child in table["children"]:
                child_records = select_query_pk(source_conx, child["name"], child["fk"], record)
                child_columns = get_columns(source_conx, child["target_table"])
                for child_record in child_records:
                    insert_update_query(child_record, target_conx, child['target_table'], child["pk"], child_columns)
        json_last_runtimes[table_name] = now
        write_file("./database_info.json", json_database_info)
        write_file("./last_runtimes.json", json_last_runtimes)
    
    db_close(source_conx)
    db_close(target_conx)
        
    
main()
