import pyodbc

def fetch_top_orders():
    # Define the connection string
    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=192.168.0.134,1433;"  # Explicitly specify the port
        "Database=WideWorldImporters;"
        "UID=my_username;"      # Replace with SQL Server username
        "PWD=my_password;"      # Replace with SQL Server password
    )

    try:
        # Establish the connection
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            # Define the query
            query = "SELECT TOP 5 * FROM Sales.Orders;"
            
            # Execute the query
            cursor.execute(query)
            
            # Fetch and print the results
            rows = cursor.fetchall()
            for row in rows:
                print(row)
    except pyodbc.Error as e:
        print("Error connecting to SQL Server:", e)

if __name__ == "__main__":
    fetch_top_orders()