import json
import yaml
import time
import boto3
import math
import os
import datetime
import concurrent.futures
import base64
from botocore.exceptions import ClientError

configuration_file = 'lambda/serverless.yml'
status_file = 'lambda/status.json'
result_folder = 'result'


def get_config():

    with open('lambda/serverless.yml', 'r') as sls_config:
        config = yaml.load(sls_config.read())

    return config


def invoke_lambda(function_name, region_name, payload, invocation_type, log_type='None'):
    lambda_client = boto3.client('lambda', region_name=region_name)

    return lambda_client.invoke(FunctionName=function_name,
                                InvocationType=invocation_type,
                                Payload=payload,
                                LogType=log_type)


def get_log_events(log_group_name, filter_pattern, start_time, return_messages=False, region_name=False):

    # if no region specified use region
    if not region_name:
        config = get_config()
        region_name = config['custom']['aws_region']

    log_client = boto3.client('logs', region_name=region_name)
    response = log_client.filter_log_events(logGroupName=log_group_name,
                                            filterPattern=filter_pattern,
                                            startTime=start_time)
    num_events = len(response.get('events', []))
    messages = [event['message'] for event in response.get('events', [])]

    # loop through finding all logs
    while response.get('nextToken', False):
        response = log_client.filter_log_events(logGroupName=log_group_name,
                                                filterPattern=filter_pattern,
                                                startTime=start_time,
                                                nextToken=response['nextToken'])
        num_events += len(response.get('events', []))
        new_messages = [event['message'] for event in response.get('events', [])]
        messages.append(new_messages)

    if return_messages:
        return messages
    else:
        return num_events


def check_lambdas(function_name, num_invocations, start_time, region_name=False, sleep_time=3):

    log_group_name = '/aws/lambda/{}'.format(function_name)
    print("Checking Lambdas in {}".format(region_name))
    num_lambdas_started = 0
    num_lambdas_ended = 0

    while True:
        time.sleep(sleep_time)
        if num_lambdas_ended >= num_invocations:
            print('All lambdas ended!')
            break
        else:
            num_lambdas_ended = get_log_events(log_group_name=log_group_name,
                                               filter_pattern='END RequestId',
                                               start_time=start_time,
                                               return_messages=False,
                                               region_name=region_name)

        # Only check if not all lambdas are started
        if num_lambdas_started != num_invocations:
            num_lambdas_started = get_log_events(log_group_name=log_group_name,
                                                 filter_pattern='START RequestId',
                                                 start_time=start_time,
                                                 return_messages=False,
                                                 region_name=region_name)
        # Print Results
        print("{} Lambdas Invoked, {} Lambdas Started, {} Lambdas completed".format(num_invocations,
                                                                                    num_lambdas_started,
                                                                                    num_lambdas_ended))
    return True


def clear_bucket():

    config = get_config()

    bucket_name = config['custom']['bucketName']
    region = config['custom']['aws_region']

    s3_client = boto3.client('s3', region_name=region)

    kwargs = {'Bucket': bucket_name}

    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        keys = []

        for obj in resp.get('Contents', []):
            keys.append({'Key': obj['Key']})

        if len(keys) > 0:
            s3_client.delete_objects(Bucket=bucket_name,
                                     Delete={'Objects': keys})
        else:
            print("Bucket is empty.")

        # try the next iteration, (list_objects_v2 only returns first 1000 entries)
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys


def download_bucket():

    """
    Download all files from a bucket into the result_folder on local machine
    Script will delete all items in result_folder before downloading
    :return:
    """

    config = get_config()
    bucket_name = config['custom']['bucketName']
    region = config['custom']['aws_region']

    s3_client = boto3.client('s3', region_name=region)

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        keys = [content['Key'] for content in response['Contents']]
        print("Found {} files, waiting ...".format(len(keys)))
    except KeyError:
        print("No Files Found")
        return False

    print("Found %d items in S3...ending" % len(keys))
    s3 = boto3.resource('s3', region_name=region)
    print("Downloading all files from bucket")

    # delete all items in the result folder on local machine, and download bucket
    list(map(os.unlink, (os.path.join(result_folder, f) for f in os.listdir(result_folder))))
    for key in keys:
        s3.Bucket(config['bucket_name']).download_file(key, result_folder + '/{}'.format(key))


def async_in_region(function_name, payloads, region_name=False, max_workers=1, sleep_time=3):

    """
    Invokes lambda functions asynchronously in on region
    Number of functions invoke is equal to number of elements in Payloads list

    :param function_name:  Function Name to Invoke
    :param payloads: List of payloads (1 per function)
    :param region_name: AWS_Region to invoke in
    :param max_workers: Max number of parallel processes to use for invocations
    :param sleep_time: Time to sleep before polling for lambda status
    :return:
    """

    # if no region specified use region
    if not region_name:
        config = get_config()
        region_name = config['custom']['aws_region']

    lambda_client = boto3.client('lambda', region_name=region_name)

    print("{} functions to be invoked, reserving concurrency".format(len(payloads)))
    response = lambda_client.put_function_concurrency(FunctionName=function_name,
                                                      ReservedConcurrentExecutions=len(payloads)+1)
    print("{} now has {} reserved concurrent executions".format(function_name,
                                                                response['ReservedConcurrentExecutions']))

    print("Invoking Lambdas in {}".format(region_name))
    start_time = int(datetime.datetime.now().timestamp() * 1000)  # Epoch Time

    response = {'result': None}
    # Start invocations -- thank you @ustayready for this piece of insight :)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for k, payload in enumerate(payloads):
            response = executor.submit(lambda_client.invoke,
                                       FunctionName=function_name,
                                       InvocationType='Event',
                                       Payload=json.dumps(payload))

    print("INFO: {} Lambdas invoked, checking status\n".format(len(payloads)))
    check_lambdas(function_name=function_name,
                  num_invocations=len(payloads),
                  start_time=start_time,
                  region_name=region_name,
                  sleep_time=sleep_time)

    try:
        lambda_client.delete_function_concurrency(FunctionName=function_name)
        print("Reserved Concurrency for {} removed".format(function_name))
    except ClientError:
        pass  # no concurrency set

    return response.result()


def sync_in_region(function_name, payloads, region_name=False, max_workers=1, log_type='None'):

    # if no region specified use region
    if not region_name:
        config = get_config()
        region_name = config['custom']['aws_region']

    lambda_client = boto3.client('lambda', region_name=region_name)
    print("Invoking Lambdas in {}".format(region_name))

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(lambda_client.invoke,
                                   FunctionName=function_name,
                                   InvocationType="RequestResponse",
                                   LogType=log_type,
                                   Payload=json.dumps(payload)) for payload in payloads]

        for future in concurrent.futures.as_completed(futures):
            resp_payload = future.result()['Payload'].read().decode('utf-8')

            if log_type == 'None':
                results.append(resp_payload)
            else:
                log_result = base64.b64decode(future.result()['LogResult'])
                results.append({'resp_payload': resp_payload,
                                'log_result': log_result})

    return results
