# Environment Variables Setup

## Required Environment Variables

### SQL Server Connection
```bash
export SQL_SERVER='SQL Server IP address'        # SQL Server IP address
export SQL_DATABASE='Database name'              # Database name
export SQL_USER='SQL Server username'            # SQL Server username
export SQL_PASSWORD='SQL Server password'        # SQL Server password
export SQL_TIMEOUT='10'                   # Query timeout in seconds
export SQL_LOGIN_TIMEOUT='10'             # Connection timeout in seconds
```

### Anthropic API
```bash
export ANTHROPIC_API_KEY='your-api-key-here'  # Claude API key
```

## Setting Up

1. Copy this template
2. Replace the values with your actual credentials
3. Save it as `env.md` (this file is gitignored)
4. Source the variables before running the script:
   ```bash
   source env.md
   ```

## Security Notes
- Never commit credentials to version control
- Consider using a proper secrets management system in production
- Rotate credentials regularly
- Use the minimum required permissions for the SQL user

## Connection Management
The script implements connection pooling to:
- Reuse existing connections when possible
- Automatically handle dead connections
- Reduce connection overhead
- Properly close connections when done
