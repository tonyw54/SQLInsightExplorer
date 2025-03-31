import pytds
from contextlib import contextmanager
import socket
from datetime import datetime
import os

@contextmanager
def get_connection():
    """Create a context-managed SQL Server connection."""
    conn = None
    try:
        # Using IP address with default instance (MSSQLSERVER)
        conn = pytds.connect(
            server='192.168.0.144',  # IP address only - default instance doesn't need instance name
            database="WideWorldImporters",
            user="my_username",
            password="my_password",
            timeout=10,              # Connection timeout
            login_timeout=10         # Login timeout
        )
        yield conn
    except socket.timeout:
        print("Connection timed out. Please check if:")
        print("1. SQL Server service (MSSQLSERVER) is running")
        print("2. TCP/IP protocol is enabled in SQL Server Configuration Manager")
        print("3. Port 1433 is open in the firewall")
        raise
    except pytds.Error as e:
        print(f"Database connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()

def fetch_top_orders(limit=5):
    """Fetch top orders from the database."""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Improved query with better date formatting and more meaningful columns
            query = """
                SELECT TOP (%d)
                    o.OrderID,
                    o.CustomerID,
                    o.OrderDate,
                    o.ExpectedDeliveryDate,
                    o.CustomerPurchaseOrderNumber,
                    CASE 
                        WHEN o.IsUndersupplyBackordered = 1 THEN 'Yes'
                        ELSE 'No'
                    END as IsBackordered
                FROM Sales.Orders o
                ORDER BY o.OrderDate DESC
            """ % limit
            
            cursor.execute(query)
            
            # Fetch and print results with better formatting
            for row in cursor:
                order_date = row[2].strftime('%Y-%m-%d') if row[2] else 'N/A'
                delivery_date = row[3].strftime('%Y-%m-%d') if row[3] else 'N/A'
                po_number = row[4] if row[4] else 'N/A'
                
                print(f"Order ID: {row[0]}")
                print(f"  Customer ID: {row[1]}")
                print(f"  Order Date: {order_date}")
                print(f"  Expected Delivery: {delivery_date}")
                print(f"  PO Number: {po_number}")
                print(f"  Backordered: {row[5]}")
                print("-" * 50)
                
    except pytds.Error as e:
        print(f"Error executing query: {e}")

if __name__ == "__main__":
    fetch_top_orders()