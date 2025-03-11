import duckdb
import os

# Path to your SQLite database
sqlite_db_path = os.path.expanduser("~/workspace/p1-to-duckdb/e_historie.db")

# Path for the new DuckDB database
duckdb_path = "p1.ddb"

# Connect to DuckDB
con = duckdb.connect(duckdb_path)

try:
    # Attach the SQLite database in read-only mode
    con.execute(f"ATTACH '{sqlite_db_path}' AS sqlite_db (TYPE sqlite, READ_ONLY)")

    query = "select * from sqlite_db.e_history_min limt 5;"

    result = con.execute(query)

    for line in result:
        print(line)
    
    # Get the list of tables from SQLite database
    tables = con.execute("SELECT name FROM sqlite_db.sqlite_master WHERE type='table'").fetchall()

    # Copy each table from SQLite to DuckDB
    for table in tables:
        table_name = table[0]
        print(f"Copying table: {table_name}")

        # Create the table in DuckDB and copy the data
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM sqlite_db.{table_name}")

    print("Data copy completed successfully!")

    # Example: Query the e_history_min table
    result = con.execute("""
        SELECT * FROM e_history_min LIMIT 5
    """).fetchall()

    print("\nSample data from e_history_min:")
    for row in result:
        print(row)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Detach the SQLite database
    con.execute("DETACH sqlite_db")

    # Close the DuckDB connection
    con.close()
