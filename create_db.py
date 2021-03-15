import re
import sys
import logging
from time import sleep, time
from os import path, mkdir, listdir, remove as remove_file

import psycopg2
import matplotlib.pyplot as plt


# Logger init
logging.basicConfig(level=logging.DEBUG, filename='create_db.log', format='%(asctime)s %(levelname)s:%(message)s')

# Helper functions
def get_filenames(dir_name):
    if not path.exists(dir_name):
        return []

    filenames = listdir(dir_name)
    filenames = [dir_name + f"/{dir_elem}" for dir_elem in filenames]
    return filenames

def get_column_names_types(filename):
    column_names_list = []
    column_names_list_type = []
    with open(filename, 'r') as csv_f:
        column_names_list = csv_f.readline().strip().replace('"', '').split(';')
    column_names_list.append('EXAMYEAR')
    
    for idx, elem in enumerate(column_names_list):
        if 'id' in elem.lower():
            column_names_list_type.append(elem + " VARCHAR(36) PRIMARY KEY")
        elif 'bal' in elem.lower():
            column_names_list_type.append(elem + " REAL")
        elif elem.lower() == 'birth' or elem.lower() == 'examyear':
            column_names_list_type.append(elem + " INTEGER")
        else:
            column_names_list_type.append(elem + " VARCHAR(500)")
    
    return column_names_list, column_names_list_type

def query_to_csv(results, result_csv_name = "query_result.csv"):
    data = [("REGION", "MIN_RESULT", "EXAMYEAR")] + results

    with open("results/" + result_csv_name, "w") as result_csv:
        for result in data:
            result_csv.write(','.join([str(elem) for elem in result]) + "\n")

def query_plot(results, result_png_name = "query_result_compare.png", width = 0.25):
    x_labels = []
    years_data = {}

    for region, mark, year in results:
        region = region.split(' ')
        if region[0] not in x_labels:
            x_labels.append(region[0])

        if year not in years_data.keys():
            years_data[year] = [mark]
        else:
            years_data[year].append(mark)

    x_labels_len_iter = range(len(x_labels))

    fig, ax = plt.subplots(figsize = (16, 9))

    idx = 0
    for year, marks_list in years_data.items():
        plt.bar([elem + idx * width for elem in x_labels_len_iter], marks_list, width, label = str(year))
        idx += 1

    plt.grid(alpha = 0.6)
    plt.xlabel('Регіон')
    plt.ylabel('Оцінка')
    plt.title('Мінімальний бал в різних областях в різні роки.')

    plt.xticks([elem + width / len(years_data.keys()) for elem in x_labels_len_iter], x_labels)
    for tick in ax.get_xticklabels():
        tick.set_rotation(30)
    plt.legend(loc = 'best')

    plt.savefig("results/" + result_png_name, dpi = fig.dpi)
    logging.info("Graph builded.")
    print("Graph builded.")

def drop_table_by_name(conn, table_name):
    with conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS %s" % table_name)
    conn.commit()


# Create connection
def db_connection(dbname, username, password, host, port, reconn_num = 10):
    conn = None
    
    # Create connection
    for idx in range(reconn_num + 1):
        try:
            conn = psycopg2.connect(dbname = dbname,
                                    user = username, 
                                    password = password,
                                    host = host,
                                    port = port)
            print("Connection created.")
            logging.info("Connection created.")
            break
        except Exception as e:
            #print(f"Connection errors: {e}", end = '\r')
            if idx != reconn_num:
                logging.error(f"Connection error. Try to reconnect. Try {idx + 1}")
                print(f"Connection error. Try to reconnect. Try {idx + 1}")
                sleep(2)
    
    return conn

def create_db_table(conn, filenames, table_name = "ZNODATA"):
    bool_created = False

    column_names_list, column_names_list_type = get_column_names_types(filenames[0])
    
    # Create sql statement for table creation
    create_table_sql_statements = f"CREATE TABLE {table_name} ("
    for col in column_names_list_type:
        create_table_sql_statements += col + ", "
    create_table_sql_statements = create_table_sql_statements[:-2] + ")"

    # Table existence script
    table_exists_sql_statements = """SELECT EXISTS(
                                SELECT 1 AS QUERY_RESULT 
                                FROM pg_tables 
                                WHERE tablename=%s)"""

    try:
        with conn.cursor() as cur:
            cur.execute(table_exists_sql_statements, (table_name.lower(),))
            if cur.fetchone()[0]:
                logging.debug(f"Table {table_name} exists.")
                print(f"Table {table_name} exists.")
                return True

            cur.execute(create_table_sql_statements)
            logging.info(f"Table {table_name} created.")
            print(f"Table {table_name} created.")

            conn.commit()
            bool_created = True
    except Exception as e:
        conn.close()
        logging.error("Problems with creating table: " + e)
        print(e)

    return bool_created

def create_db_table_helper(conn, helper_table_name = "ZNODATA_ROWTABLE"):
    bool_created = False

    create_helper_table_sql_statements = f"CREATE TABLE {helper_table_name}(row_num INTEGER PRIMARY KEY)"

    # Table existence script
    table_exists_sql_statements = """SELECT EXISTS(
                                SELECT 1 AS QUERY_RESULT 
                                FROM pg_tables 
                                WHERE tablename=%s)"""

    try:
        with conn.cursor() as cur:
            cur.execute(table_exists_sql_statements, (helper_table_name.lower(),))
            if cur.fetchone()[0]:
                logging.debug(f"Table {helper_table_name} exists.")
                print(f"Table {helper_table_name} exists.")
                return True

            cur.execute(create_helper_table_sql_statements)
            logging.info(f"Table {helper_table_name} created.")
            print(f"Table {helper_table_name} created.")

            conn.commit()
            bool_created = True
    except Exception as e:
        conn.close()
        logging.error("Problems with creating helper table: " + e)
        print(e)

    return bool_created

# Transactions
def db_insert_data_to_db(conn, filenames, dbname, username, password, host, port, table_name = "ZNODATA", helper_table_name = "ZNODATA_ROWTABLE", batch_size = 1000):
    db_helper_row = 0
    column_names_list, _ = get_column_names_types(filenames[0])

    # SQL queries
    insert_data_sql_statements = f"INSERT INTO {table_name}(" + ', '.join([col for col in column_names_list]) + ") VALUES(" + len(column_names_list) * "%s, "
    insert_data_sql_statements = insert_data_sql_statements[:-2] + ")"

    retrieve_col_row_sql_statements = f"SELECT row_num FROM {helper_table_name}"

    insert_col_row_sql_statements = f"INSERT INTO {helper_table_name}(row_num) VALUES(%s)"
    update_col_row_sql_statements = f"UPDATE {helper_table_name} SET row_num = %s"
    
    # Retrieve col num
    with conn:
        with conn.cursor() as cur:
            cur.execute(retrieve_col_row_sql_statements)
            result = cur.fetchone()
            if result is not None:
                db_helper_row = result[0]
            else:
                cur.execute(insert_col_row_sql_statements, (db_helper_row,))
                conn.commit()

    # Insert files
    start_time = time()
    idx = 0
    for filename in filenames:
        exam_year = re.findall(r'\d+', filename)[0]
        with open(filename, 'r', encoding="cp1251") as csv_data:
            csv_line = csv_data.readline()

            while csv_line != "":
                connect_bool = True
                batch_arr = []
                for i in range(batch_size):
                    csv_line = csv_data.readline()

                    if csv_line is None or csv_line == "":
                        logging.debug("End of file reached.")
                        break

                    if db_helper_row > idx:
                        idx += 1
                        continue

                    line_to_insert = csv_line.strip().replace('"', '').replace(",", '.').split(';')
                    line_to_insert = [None if elem == "null" else elem for elem in line_to_insert]
                    line_to_insert.append(exam_year)
                    
                    batch_arr.append(line_to_insert)
                    idx += 1

                while connect_bool:
                    try:
                        with conn:
                            with conn.cursor() as cur:
                                for record in batch_arr:
                                    cur.execute(insert_data_sql_statements, record)

                                if db_helper_row <= idx:
                                    db_helper_row = idx
                                    logging.info(f"Batch {idx} inserted.")
                                    cur.execute(update_col_row_sql_statements, (idx,))
                                    conn.commit()
                                connect_bool = False
                    
                    except (psycopg2.OperationalError, psycopg2.DatabaseError, psycopg2.InterfaceError) as e:
                        sleep(1)
                        conn = db_connection(dbname, username, password, host, port)
                        if conn is None:
                            return conn, False
                        logging.debug("Reconnected succesfully.")
                        connect_bool = True

                    except (psycopg2.ProgrammingError, psycopg2.DataError) as e:
                        conn.rollback()
                        return conn, False
    end_time = time()
    logging.info(f"INSERTION TIME: {end_time - start_time} s")
    print(f"INSERTION TIME: {end_time - start_time} s")
    
    # Drop temp helper table
    drop_table_by_name(conn, "ZNODATA_ROWTABLE")

    conn.commit()
    return conn, True

# SQL Query result => .csv
def db_sql_execute_save(conn):
    sql_query_select = """
            SELECT
                regname AS REGION,
                MIN(engball100) AS MIN_RESULT,
                EXAMYEAR
            FROM ZNODATA
            WHERE 
                engball100 IS NOT NULL
            AND
                engteststatus = 'Зараховано'
            GROUP BY regname, EXAMYEAR
            """
    
    with conn.cursor() as cur:
        cur.execute(sql_query_select)
        query_results = cur.fetchall()
        query_to_csv(query_results)
        query_plot(query_results)
        logging.info("Query results added to csv.")
        print("Query results added to csv.")


def main():
    # DB params
    python_file, dbname, username, password, host, port = sys.argv
    
    # Return filenames
    filenames = get_filenames("data")
    if filenames == []:
        logging.error("Directory with data is empty. Exit program.")
        print("Directory with data is empty. Exit program.")
        return

    # Create connection (else: exit script)
    conn = db_connection(dbname, username, password, host, port)
    if conn is None:
        logging.error("Connection failed. Exit program.")
        print("Connection failed. Exit program.")
        return

    # Create main table in db
    bool_created = create_db_table(conn, filenames)
    if not bool_created:
        logging.error("Error in creating table. Exit Program.")
        print("Error in creating table. Exit Program.")
        return

    # Create helper table in db
    bool_created = create_db_table_helper(conn)
    if not bool_created:
        logging.error("Error in creating helper table. Exit Program.")
        print("Error in creating helper table. Exit Program.")
        return

    # Insert data
    conn, bool_inserted = db_insert_data_to_db(conn, filenames, dbname, username, password, host, port)
    if not bool_inserted:
        logging.error("Data insertion error. Exit Program.")
        print("Data insertion error. Exit Program.")
        return

    # Execute Computer Practice 1 queries
    db_sql_execute_save(conn)

    # Drop main table (uncomment if you need this)
    # drop_table_by_name(conn, "ZNODATA")

    if conn is not None:
        conn.close()
    logging.info("Connection closed.")
    print("Connection closed.")


if __name__ == "__main__":
    main()
