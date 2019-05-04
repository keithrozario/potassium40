import boto3
import logging


def check_execution_status(execution_id, client):
    """
    checks the execution status of a Athena Query
    loops until a result is available
    returns result
    """

    state = 'RUNNING'
    while state == 'RUNNING':
        result = client.get_query_execution(QueryExecutionId=execution_id)
        state = result['QueryExecution']['Status']['State']

    if state == 'SUCCEEDED':
        location = result['QueryExecution']['ResultConfiguration']['OutputLocation']
    else:
        location = result['QueryExecution']['Status']['StateChangeReason']

    return state, location


def create_athena_db(bucket_name, region):
    """
    creates and Athena database and table
    Database name hardcoded to p40
    Table name hardcoded to robots
    """

    logger = logging.getLogger('__main__')
    client = boto3.client('athena', region_name=region)
    workgroup = 'primary'

    create_db_query = f"CREATE DATABASE IF NOT EXISTS p40 LOCATION 's3://{bucket_name}'"
    create_table_query = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS p40.robots (
      `domain` string,
      `robots.txt` string 
    )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    WITH SERDEPROPERTIES (
      'serialization.format' = '1'
    ) LOCATION 's3://<bucket_name>/robots/'
    TBLPROPERTIES ('has_encrypted_data'='false');
    '''.replace('<bucket_name>', bucket_name)

    queries = [create_db_query, create_table_query]
    logger.info('Creating Athena Database and Tables')
    for k, query in enumerate(queries):
        response = client.start_query_execution(QueryString=query,
                                                ResultConfiguration={'OutputLocation': f"s3://{bucket_name}/athena"},
                                                WorkGroup=workgroup)
        result, location = check_execution_status(response['QueryExecutionId'], client)
        if result != 'SUCCEEDED':
            logger.info(f"Failed Executing {query}")
            break
        else:
            logger.info(f"Succeeded. Location: {location}")


def query_robots(bucket_name, region):
    """
    Queries table and returns location where result file is available
    database and table name hardcoded to p40
    """
    query = 'select * from p40.robots ORDER BY domain'
    workgroup = 'primary'

    client = boto3.client('athena', region_name=region)
    logger = logging.getLogger('__main__')

    response = client.start_query_execution(QueryString=query,
                                            ResultConfiguration={'OutputLocation': f"s3://{bucket_name}/athena"},
                                            QueryExecutionContext={'Database': 'p40'},
                                            WorkGroup=workgroup)
    result, location = check_execution_status(response['QueryExecutionId'], client)
    if result != 'SUCCEEDED':
        logger.info(f"Failed Executing {query}")
    else:
        logger.info(f"Succeeded in Querying bucket. Result Location: {location}")

    return location
