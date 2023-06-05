import os
import csv
import psycopg2

data_folder = "data"
host = "localhost"
database = "postgres"
user = "postgres"
password = "Moon2far"
port = 5432


def detect_data_type(data):
    try:
        int(data)
        return "integer"
    except ValueError:
        try:
            float(data)
            return "numeric"
        except ValueError:
            return "text"


def create_table(cursor, create_table_sql):
    table_name = create_table_sql.split()[2]  # Extract table name from SQL statement
    check_table_exists_sql = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"
    cursor.execute(check_table_exists_sql)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        cursor.execute(create_table_sql)
        print(f"Table {table_name} created successfully.")
    else:
        cursor.execute(f"DROP TABLE {table_name}")
        cursor.execute(create_table_sql)
        print(f"Table {table_name} replaced successfully.")


def insert_data(cursor, table_name, columns, data):
    insert_sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})"
    cursor.executemany(insert_sql, data)
    print(f"Inserted {len(data)} rows into {table_name}.")


def main():
    conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
    cursor = conn.cursor()

    csv_files = [file for file in os.listdir(data_folder) if file.endswith(".csv")]

    for file in csv_files:
        table_name = file.split(".")[0]
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("

        with open(os.path.join(data_folder, file), "r") as csv_file:
            reader = csv.reader(csv_file)
            header = next(reader)
            columns = []
            primary_key = None
            foreign_keys = []

            for column in header:
                column_name = column.strip().lower().replace(" ", "_")
                data_type = detect_data_type(column)
                is_primary_key = False
                is_foreign_key = False

                # Detect primary key
                if column_name == f"{table_name}_id":
                    data_type = "serial"
                    is_primary_key = True
                    primary_key = column_name

                columns.append(column_name)

            for column in columns:
                create_table_sql += f"{column} text,"

            # Add primary key
            if primary_key:
                create_table_sql += f"PRIMARY KEY ({primary_key}),"

            # Add foreign keys
            for foreign_key in foreign_keys:
                column_name, referenced_table = foreign_key
                create_table_sql += f"FOREIGN KEY ({column_name}) REFERENCES {referenced_table},"

            create_table_sql = create_table_sql.rstrip(",") + ")"
            create_table(cursor, create_table_sql)

            # Insert data
            data = []
            for row in reader:
                data.append(tuple(row))

            insert_data(cursor, table_name, columns, data)

            conn.commit()

        print(f"Table {table_name} created/replaced successfully.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
