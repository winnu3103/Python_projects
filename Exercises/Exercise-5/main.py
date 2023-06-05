import os
import csv
import psycopg2
from psycopg2 import extras


def get_data_types(data):
    data_types = {}
    for row in data:
        for key, value in row.items():
            if key not in data_types:
                try:
                    int(value)
                    data_types[key] = "INTEGER"
                except ValueError:
                    try:
                        float(value)
                        data_types[key] = "FLOAT"
                    except ValueError:
                        data_types[key] = "TEXT"
    return data_types


def detect_primary_key(data):
    primary_key = None
    first_row = data[0]
    for key, value in first_row.items():
        if key.lower() == "id":
            primary_key = key
            break
    return primary_key


def detect_foreign_key(data, table_name):
    foreign_key = None
    # Perform your logic to detect foreign key here
    return foreign_key


def generate_create_statement(table_name, data_types, primary_key=None, foreign_key=None):
    columns = []
    for column, data_type in data_types.items():
        columns.append(f"{column} {data_type}")
    if primary_key:
        columns.append(f"PRIMARY KEY ({primary_key})")
    if foreign_key:
        columns.append(f"FOREIGN KEY ({foreign_key}) REFERENCES parent_table(parent_column)")
    create_statement = f"CREATE TABLE {table_name} ({', '.join(columns)})"
    return create_statement


def create_tables(conn):
    cursor = conn.cursor()
    # Add your table creation code here
    cursor.close()


def ingest_csv_data(conn):
    cursor = conn.cursor()
    # Add your CSV data ingestion code here
    cursor.close()


def main():
    host = "localhost"
    database = "postgres"
    user = "postgres"
    password = "Moon2far"
    port = 5432

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )

    data_folder = "data"
    csv_files = [file for file in os.listdir(data_folder) if file.endswith(".csv")]

    for file in csv_files:
        table_name = file.split(".")[0]
        csv_path = os.path.join(data_folder, file)

        with open(csv_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            data = [row for row in csv_reader]

            # Detect data types for each column
            data_types = get_data_types(data)

            # Detect primary key
            primary_key = detect_primary_key(data)

            # Detect foreign key
            foreign_key = detect_foreign_key(data, table_name)

            # Generate CREATE statement
            create_statement = generate_create_statement(table_name, data_types, primary_key, foreign_key)

            # Create table
            create_tables(conn)
            cursor = conn.cursor()
            cursor.execute(create_statement)

            # Ingest CSV data
            ingest_csv_data(conn)
            insert_statement = f"INSERT INTO {table_name} ({','.join(data_types.keys())}) VALUES %s"
            values = [tuple(row.values()) for row in data]
            extras.execute_values(cursor, insert_statement, values)

            conn.commit()
            cursor.close()

    conn.close()


if __name__ == "__main__":
    main()
