import json
import boto3
import pyodbc
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secret(secret_name):
    """
    Retrieve secret from AWS Secrets Manager
    """
    # Create a Secrets Manager client
    client = boto3.client('secretsmanager')
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        logger.error(f"Error retrieving secret: {e}")
        raise e
    
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

def connect_to_mssql(secret):
    """
    Connect to MS SQL Server using credentials from Secrets Manager
    """
    try:
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={secret['host']};"
            f"DATABASE={secret['database']};"
            f"UID={secret['username']};"
            f"PWD={secret['password']}"
        )
        return connection
    except Exception as e:
        logger.error(f"Error connecting to MS SQL Server: {e}")
        raise e

def lambda_handler(event, context):
    """
    Lambda function handler
    """
    secret_name = "your_secret_name"
    
    try:
        secret = get_secret(secret_name)
        connection = connect_to_mssql(secret)
        
        # Example query execution
        cursor = connection.cursor()
        cursor.execute("SELECT @@VERSION;")
        row = cursor.fetchone()
        
        logger.info(f"Database version: {row[0]}")
        
        # Close the connection
        cursor.close()
        connection.close()
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Database version: {row[0]}")
        }
    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {e}")
        }
