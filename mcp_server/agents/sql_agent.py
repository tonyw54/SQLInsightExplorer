import os
import pytds
import anthropic
from typing import Dict, Any, List, Optional
from contextlib import contextmanager

class SQLTool:
    def __init__(self):
        """Initialize SQLTool with API client and connection parameters."""
        # Get API key from environment variable
        api_key = os.getenv('ANTHROPIC_API_KEY')
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable is not set")
        
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = "claude-3-7-sonnet-20250219"
        
        # Cache connection parameters
        self.server = os.getenv('SQL_SERVER')
        self.database = os.getenv('SQL_DATABASE')
        self.user = os.getenv('SQL_USER')
        self.password = os.getenv('SQL_PASSWORD')
        self.timeout = int(os.getenv('SQL_TIMEOUT', '10'))
        self.login_timeout = int(os.getenv('SQL_LOGIN_TIMEOUT', '10'))
        
        # Validate connection parameters
        if not all([self.server, self.database, self.user, self.password]):
            raise ValueError("Missing required SQL connection environment variables. Please set: SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD")
        
        # Connection pool
        self._conn = None
    
    @contextmanager
    def _get_connection(self) -> pytds.Connection:
        """Get a database connection from the pool or create a new one.
        
        Returns:
            pytds.Connection: Database connection object
            
        Raises:
            ValueError: If connection parameters are missing
            pytds.Error: If connection fails
        """
        try:
            # Reuse existing connection if it's alive
            if self._conn is not None:
                try:
                    # Test connection with a simple query
                    cursor = self._conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                    yield self._conn
                    return
                except Exception:
                    # Connection is dead, close it
                    try:
                        self._conn.close()
                    except Exception:
                        pass
                    self._conn = None
            
            # Create new connection
            self._conn = pytds.connect(
                server=self.server,
                database=self.database,
                user=self.user,
                password=self.password,
                timeout=self.timeout,
                login_timeout=self.login_timeout
            )
            yield self._conn
            
        except Exception as e:
            self._conn = None
            raise ConnectionError(f"Failed to connect to database: {str(e)}")
        
    def get_table_schema(self) -> Dict[str, List[Dict[str, str]]]:
        """Get schema information for all tables in the database"""
        tables = {}
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Get all tables
                cursor.execute("""
                    SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE'
                """)
                
                table_names = [row[0] for row in cursor.fetchall()]
                
                # Get columns for each table
                for table in table_names:
                    schema, table_name = table.split(".")
                    cursor.execute(f"""
                        SELECT COLUMN_NAME, DATA_TYPE
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema}'
                    """)
                    
                    columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
                    tables[table] = columns
                    
                cursor.close()
                
        except Exception as e:
            print(f"Error getting schema: {e}")
            return {}
            
        return tables
    
    def _format_schema_for_prompt(self, schema: Dict[str, List[Dict[str, str]]]) -> str:
        """Format schema for the prompt"""
        schema_str = []
        for table, columns in schema.items():
            col_str = ", ".join([f"{col['name']} ({col['type']})" for col in columns])
            schema_str.append(f"{table}: {col_str}")
        return "\n".join(schema_str)

    def generate_sql_query(self, natural_language_query: str) -> str:
        """Convert natural language to SQL using Claude"""
        schema = self.get_table_schema()
        if not schema:
            return "ERROR: Could not retrieve database schema"

        schema_str = self._format_schema_for_prompt(schema)
        
        prompt = f"""
        You are a SQL query generator. Given the following database schema and a natural language question, 
        generate the appropriate SQL query to answer the question.
        
        DATABASE SCHEMA:
        {schema_str}
        
        QUESTION:
        {natural_language_query}
        
        Generate only the SQL query without any explanations or markdown formatting. The query should be valid for SQL Server.
        Do not include ```sql or ``` markers. Return only the raw SQL query.
        """
        
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1000,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Extract SQL query from response and remove markdown if present
            sql_query = response.content[0].text.strip()
            sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
            
            # Basic protection against harmful queries
            if any(keyword in sql_query.lower() for keyword in ["drop", "truncate", "delete", "update", "insert", "create"]):
                return "ERROR: Potentially harmful query detected. Only SELECT queries are allowed."
            
            return sql_query
        except Exception as e:
            return f"ERROR: Failed to generate query: {str(e)}"
    
    def execute_query(self, sql_query: str) -> Dict[str, Any]:
        """Execute the generated SQL query and return results"""
        result = {"status": "error", "query": sql_query, "error": None, "data": None}
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute(sql_query)
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
                
                result["status"] = "success"
                result["data"] = {
                    "columns": columns,
                    "rows": [[str(cell) for cell in row] for row in rows]
                }
                
                cursor.close()
                
        except Exception as e:
            result["error"] = str(e)
            
        return result

    def natural_language_to_sql_results(self, natural_language_query: str) -> Dict[str, Any]:
        """Process natural language query to SQL results in one step"""
        sql_query = self.generate_sql_query(natural_language_query)
        
        if sql_query.startswith("ERROR"):
            return {"status": "error", "error": sql_query}
        
        return self.execute_query(sql_query)
    
# Example usage
if __name__ == "__main__":
    sql_tool = SQLTool()
    
    # Example: Get the top 5 orders with their details
    query = """
    Show me the top 5 most recent orders with their order date, customer ID, and purchase order number
    """
    
    # Generate and execute query
    sql_query = sql_tool.generate_sql_query(query)
    result = sql_tool.execute_query(sql_query)
    
    # Pretty print the results
    print("\nGenerated SQL Query:")
    print("-" * 80)
    print(sql_query)
    print("-" * 80)
    
    if result["status"] == "success" and result["data"]:
        print("\nResults:")
        print("-" * 80)
        # Print column headers
        columns = result["data"]["columns"]
        rows = result["data"]["rows"]
        
        # Calculate column widths
        widths = [max(len(str(row[i])) for row in rows + [columns]) for i in range(len(columns))]
        
        # Print headers
        header = " | ".join(f"{col:{width}}" for col, width in zip(columns, widths))
        print(header)
        print("-" * len(header))
        
        # Print rows
        for row in rows:
            print(" | ".join(f"{str(val):{width}}" for val, width in zip(row, widths)))
    else:
        print("\nError:")
        print(result["error"])