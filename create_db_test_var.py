import sys
import time
import shutil
import psycopg2
from math import ceil
from typing import List
from os import path, mkdir, listdir, remove as remove_file


def get_column_names_types(filename):
    column_names_list = []
    column_names_list_type = []
    with open(filename, 'r') as csv_f:
        column_names_list = csv_f.readline().strip().replace('"', '').split(';')
    
    for idx, elem in enumerate(column_names_list):
        if 'id' in elem.lower():
            column_names_list_type.append(elem + " VARCHAR(36) PRIMARY KEY")
        elif 'bal' in elem.lower():
            column_names_list_type.append(elem + " REAL")
        elif elem.lower == 'birth':
            column_names_list_type.append(elem + " INTEGER")
        else:
            column_names_list_type.append(elem + " VARCHAR(500)")
    
    return column_names_list, column_names_list_type

def create_batches(filenames, batched_data_dir_name = "batched_data", batch_size = 1000):
    batch_name = "csv_batch"
    start = time.time()
    
    # Create dir for batches or
    # Clean dir if data in dir exists
    if not path.exists(batched_data_dir_name):
        mkdir(batched_data_dir_name)
    
    files_list = listdir(batched_data_dir_name)
    if files_list:
        for file in files_list:
            remove_file(path.join(batched_data_dir_name, file))
    
    batch_num = 0
    for filename in filenames:
        with open(filename, "r") as csv_f:
            csv_f_lines_idx = 0
            csv_f_lines = csv_f.readlines()[1:]
            csv_f_lines_len = len(csv_f_lines)
            csv_f_batches_num = ceil(csv_f_lines_len / batch_size)
            
            for idx in range(csv_f_batches_num):
                with open(batched_data_dir_name + "/" + batch_name + f"_{batch_num}.csv", "w") as csv_batch:
                    for jdx in range(batch_size):
                        line = csv_f_lines[csv_f_lines_idx]
                        if line is None or csv_f_lines_idx + 1 == csv_f_lines_len:
                            break
                        csv_batch.write(line)
                        csv_f_lines_idx += 1
                batch_num += 1

    print(f"TIME OF BATCHING: {round(time.time() - start, 1)} seconds.")
    print("Batches created.\n")

'''def insert_data_table(cursor, table_name, filenames: List[str], columns):
    insert_query_sql = f"INSERT INTO {table_name}(" + ', '.join([col for col in columns]) + ") VALUES(" + len(columns) * "%s, "
    insert_query_sql = insert_query_sql[:-2] + ")"
    
    start = time.time()
    for filename in filenames:
        with open(filename, 'r', encoding="cp1251") as csv_f:
            for idx, line in enumerate(csv_f):
                if idx == 0: continue

                line_data = line.strip().replace('"', '').replace(",", '.').split(';')
                line_data = [None if elem == "null" else elem for elem in line_data]
                cursor.execute(insert_query_sql, line_data)

    print(f"TIME OF INSERTION: {round(time.time() - start, 1)} second")'''

def insert_data_batch_table(cursor, table_name, batch_name, columns):
    insert_query_sql = f"INSERT INTO {table_name}(" + ', '.join([col for col in columns]) + ") VALUES(" + len(columns) * "%s, "
    insert_query_sql = insert_query_sql[:-2] + ")"
    
    with open(batch_name, 'r', encoding="cp1251") as csv_f:
        for idx, line in enumerate(csv_f):
            line_data = line.strip().replace('"', '').replace(",", '.').split(';')
            line_data = [None if elem == "null" else elem for elem in line_data]
            cursor.execute(insert_query_sql, line_data)

def query_to_csv(results: List[set], result_csv_name = "query_result.csv"):
    data = [("REGION", "MIN_RESULT")] + results

    with open(result_csv_name, "w") as result_csv:
        for result in data:
            result_csv.write(','.join([str(elem) for elem in result]) + "\n")

# Connection and creation
def db_connect_create(dbname, username, password, host, port, filenames: List[str], table_name = "ZNODATA"):
    conn = None
    
    try:
        # Create connection and cursor
        conn = psycopg2.connect(dbname = dbname,
                                user = username, 
                                password = password,
                                host = host,
                                port = port)
        print("Connection created.")
        
        cur = conn.cursor()
        print("Cursor opened.")

        # Column names
        column_names_list, column_names_list_type = get_column_names_types(filenames[0])
        
        # Create table
        create_table_sql_statements = f"CREATE TABLE {table_name} ("
        for col in column_names_list_type:
            create_table_sql_statements += col + ", "
        create_table_sql_statements = create_table_sql_statements[:-2] + ")"
        
        cur.execute(create_table_sql_statements)
        print(f"Table {table_name} created.")

        conn.commit()

        # Insert data
        #insert_data_table(cur, table_name, filenames, column_names_list)
        #print("Data inserted.")

        #conn.commit()
        
        # Close cursor
        cur.close()
        print("Cursor closed.")
    
    except (Exception, psycopg2.DatabaseError) as error:
        print("Connection errors list:\n", error)
    
    finally:
        if conn is not None:
            conn.close()
            print("Connection closed.\n")

def db_insert_batched_data(dbname, username, password, host, port, filename, batched_data_dir_name = "batched_data", table_name = "ZNODATA"):
    conn = None
    batches_files_path_list = listdir(batched_data_dir_name)

    try:
        # Create connection and cursor
        conn = psycopg2.connect(dbname = dbname,
                                user = username, 
                                password = password,
                                host = host,
                                port = port)
        print("Connection created.")
        
        cur = conn.cursor()
        print("Cursor opened.")

        # Column names
        column_names_list, _ = get_column_names_types(filename)

        start = time.time()
        for filename in sorted(batches_files_path_list, key=len):
            insert_data_batch_table(cur, table_name, batched_data_dir_name + "/" + filename, column_names_list)
        print(f"TIME OF BATCH INSERTION: {round(time.time() - start, 1)} seconds.")
        conn.commit()
        
        # Close cursor
        cur.close()
        print("Cursor closed.")
    
    except (Exception, psycopg2.DatabaseError) as error:
        print("Insertion errors list:\n", error)
    
    finally:
        if conn is not None:
            conn.close()
            print("Connection closed.\n")

def db_sql_execute_save(dbname, username, password, host, port):
    conn = None
    
    try:
        # Create connection and cursor
        conn = psycopg2.connect(dbname = dbname,
                                user = username, 
                                password = password,
                                host = host,
                                port = port)
        print("Connection created.")
        
        cur = conn.cursor()
        print("Cursor opened.")

        # SQL Query
        # engteststatus - зараховано
        # engball100, regname
        # no [null] values
        # group by region - regname
        sql_query_select = """
            SELECT
                regname AS REGION,
                MIN(engball100) AS MIN_RESULT
            FROM ZNODATA
            WHERE 
                engball100 IS NOT NULL
            AND
                engteststatus = 'Зараховано'
            GROUP BY regname
            """
        cur.execute(sql_query_select)
        query_results = cur.fetchall()
        query_to_csv(query_results)
        print("Query results added to csv.")
        
        # Close cursor
        cur.close()
        print("Cursor closed.")
    
    except (Exception, psycopg2.DatabaseError) as error:
        print("Query errors list:\n", error)
    
    finally:
        if conn is not None:
            conn.close()
            print("Connection closed.\n")


if __name__ == "__main__":
    # Get cmd args
    filenames = ["data/Odata2020File.csv", "data/Odata2019File.csv"]
    python_file, dbname, username, password, host, port = sys.argv

    #db_connect_create(dbname, username, password, host, port, filenames)
    #create_batches(filenames)
    #db_insert_batched_data(dbname, username, password, host, port, filenames[0])
    db_sql_execute_save(dbname, username, password, host, port)
