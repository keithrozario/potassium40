import boto3
import json
import logging

logger = logging.getLogger()
level = logging.INFO
logger.setLevel(level)


def invoke_lambdas(event, context):

    lambda_client = boto3.client('lambda')

    # Setup Variables
    function_name = event['function_name']
    payloads = event['payloads']
    invocation_type = event.get('invocation_type', 'Event')

    logger.info("Invoking for {}:{}".format(function_name, invocation_type))

    # Invoke functions one by one
    for payload in payloads:
        logger.info("{}".format(json.dumps(payload)))
        logger.info("Payload: ")
        lambda_client.invoke(FunctionName=function_name,
                             InvocationType=invocation_type,
                             Payload=json.dumps(payload))

    return {"statusCode": 200}

