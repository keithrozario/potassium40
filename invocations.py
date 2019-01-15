import json
import yaml
import boto3
import os
import time
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


def set_concurrency(num_payloads, lambda_client, function_name):

    if num_payloads < 100:
        return None
    else:
        print("{} functions to be invoked, reserving concurrency".format(num_payloads))
        response = lambda_client.put_function_concurrency(FunctionName=function_name,
                                                          ReservedConcurrentExecutions=num_payloads + 10)
        print("{} now has {} reserved concurrent executions".format(function_name,
                                                                    response['ReservedConcurrentExecutions']))
        return None


def calc_concurrency(num_payloads):
    if num_payloads > 100:
        return num_payloads + 10
    else:
        return num_payloads


def get_log_events(client, function_name, start_time, region_name=False):

    """
    Returns the number of events that match a filter patter for the log_group from start_time till now

    """

    payload = {'function_name': function_name, 'start_time': start_time}

    response = client.invoke(FunctionName='potassium40-functions-check_lambda',
                             InvocationType='RequestResponse',
                             Payload=json.dumps(payload))

    try:
        result = json.loads(response['Payload'].read())
        lambda_count = json.loads(result['results'])

        started = lambda_count['START RequestId']
        ended = lambda_count['END RequestId']
    except KeyError:
        started = 0
        ended = 0

    return started, ended


def check_lambdas(function_name, num_invocations, start_time, region_name=False, sleep_time=3):

    print("Checking Lambdas in {}".format(region_name))
    num_lambdas_started = 0
    num_lambdas_ended = 0

    # if no region specified use region
    if not region_name:
        config = get_config()
        region_name = config['custom']['aws_region']

    client = boto3.client('lambda', region_name=region_name)

    while True:
        time.sleep(sleep_time)
        if num_lambdas_ended >= num_invocations:
            print('All lambdas ended!')
            break
        else:
            num_lambdas_started, num_lambdas_ended = get_log_events(client=client,
                                                                    function_name=function_name,
                                                                    start_time=start_time,
                                                                    region_name=region_name)
        # Print Results
        print("{} Lambdas Started, {} Lambdas completed".format(num_lambdas_started,
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


def async_in_region(function_name, payloads, region_name=False, sleep_time=3):

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

    per_lambda_invocation = 50   # each lambda will invoke 50 payloads

    # if no region specified use region
    if not region_name:
        config = get_config()
        region_name = config['custom']['aws_region']

    lambda_client = boto3.client('lambda', region_name=region_name)
    set_concurrency(len(payloads), lambda_client, function_name)

    print("\nInvoking Lambdas in {}".format(region_name))
    start_time = int(time.time() * 1000)  # Epoch Time in milliseconds

    mark = 0
    final_payloads = []

    for k, payload in enumerate(payloads):
        # split payloads to per_lambda_invocations
        if k % per_lambda_invocation == 0 and k != 0:
            final_payloads.append(payloads[mark:k])
            mark = k

    # last payload (leftover)
    final_payloads.append(payloads[mark:len(payloads)])

    for k, payload in enumerate(final_payloads):
        event = dict()
        event['function_name'] = function_name
        event['invocation_type'] = 'Event'
        event['payloads'] = payload
        lambda_client.invoke(FunctionName='potassium40-functions-invoke_lambdas',
                             InvocationType='Event',
                             Payload=json.dumps(event))
        print("INFO: Invoking lambdas {} to {}".format(k * per_lambda_invocation,
                                                       (k+1) * per_lambda_invocation))
        time.sleep(3)  # don't invoke all at once

    print("\nINFO: {} Lambdas invoked, checking status\n".format(len(payloads)))
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

    return None


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

    return None
