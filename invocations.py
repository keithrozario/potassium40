import yaml
import json
import time
import boto3
import math
import os
import datetime
import concurrent.futures
import base64
from botocore.exceptions import ClientError

configuration_file = 'lambda/serverless.yml'
status_file = 'status.json'
result_folder = 'result'
default_region = 'us-east-1'


def get_regions():

    with open(configuration_file, 'r') as serverless:
        sls_config = yaml.load(serverless.read())

    region = sls_config.get('provider', dict()).get('region', default_region)

    return {'region': region}



def get_config():

    with open(status_file, 'r') as status_reader:
        config = json.loads(status_reader.read())

    return config


def invoke_lambda(function_name, region_name, payload, invocation_type, log_type='None'):
    lambda_client = boto3.client('lambda', region_name=region_name)

    return lambda_client.invoke(FunctionName=function_name,
                                InvocationType=invocation_type,
                                Payload=payload,
                                LogType=log_type)


def get_log_events(log_group_name, filter_pattern, start_time, return_messages=False, region_name=False):

    if not region_name:
        region_name = get_regions()['region']

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

    s3_client = boto3.client('s3', region_name=get_regions()['region'])
    config = get_config()

    kwargs = {'Bucket': config['bucket_name']}

    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        keys = []

        for obj in resp.get('Contents', []):
            keys.append({'Key': obj['Key']})

        if len(keys) > 0:
            s3_client.delete_objects(Bucket=config['bucket_name'],
                                     Delete={'Objects': keys})
        else:
            print("Bucket is empty.")

        # try the next iteration, (list_objects_v2 only returns first 1000 entries)
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys


def check_bucket():

    s3_client = boto3.client('s3', region_name=get_regions()['region'])
    config = get_config()

    try:
        response = s3_client.list_objects_v2(Bucket=config['bucket_name'])
        keys = [content['Key'] for content in response['Contents']]
        print("Found {} files, waiting ...".format(len(keys)))
    except KeyError:
        print("No Files Found")
        return False

    print("Found %d items in S3...ending" % len(keys))
    s3 = boto3.resource('s3', region_name=get_regions()['region'])
    print("Downloading all files from bucket")

    # delete all items in the result folder on local machine, and download bucket
    list(map(os.unlink, (os.path.join(result_folder, f) for f in os.listdir(result_folder))))
    for key in keys:
        s3.Bucket(config['bucket_name']).download_file(key, result_folder + '/{}'.format(key))


def gen_payloads(payloads, right_invocations, per_lambda):

    final_payloads = []
    for count in range(right_invocations):
        payload = []

        try:
            for k in range(per_lambda):
                payload.append(payloads[count*per_lambda + k])
        except IndexError:
            pass  # went over the list items

        final_payloads.append(payload)

    return final_payloads


def distribute_payloads(payloads, num_invocations):
    # Get the right number of invocations
    per_lambda = int(math.ceil(len(payloads) / num_invocations))
    right_invocations = int(math.ceil(len(payloads) / per_lambda))
    print("Total payloads are {}".format(len(payloads)))
    print("Right number of invocations is {}".format(right_invocations))
    print("Each lambda will process {} payloads".format(per_lambda))
    print("Except the last lambda, which will process {} payloads\n\n".format(len(payloads) % per_lambda))

    return gen_payloads(payloads, right_invocations, per_lambda)


def consolidate_result():

    lines = []
    result_dir = "result"

    for root, dirs, files in os.walk(result_dir):
        for filename in files:
            with open(result_dir + '/' + filename, 'r') as file:
                for line in file:
                    lines.append(line)

    with open('result.txt', 'w') as output_file:
        for line in lines:
            output_file.write(line)


def async_in_region(function_name, payloads, region_name=False, max_workers=1, sleep_time=3):

    # if no region specified use region
    if not region_name:
        region_name = get_regions()['region']

    lambda_client = boto3.client('lambda', region_name=region_name)

    print("{} functions to be invoked, reserving concurrency".format(len(payloads)))
    response = lambda_client.put_function_concurrency(FunctionName=function_name,
                                                      ReservedConcurrentExecutions=len(payloads)+50)
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
        region_name = get_regions()['region']

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
