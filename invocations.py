import json
import yaml
import boto3
import os
import time
import concurrent.futures
import base64
import logging
from botocore.exceptions import ClientError

configuration_file = 'lambda/serverless.yml'
status_file = 'lambda/status.json'
result_folder = 'result'


def get_config():

    with open('lambda/serverless.yml', 'r') as sls_config:
        config = yaml.load(sls_config.read(), Loader=yaml.Loader)

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


def check_lambdas(function_name, num_invocations, start_time, region_name=False, sleep_time=3):

    print("Checking Lambdas in {}".format(region_name))
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
            num_lambdas_ended = get_log_events(client=client,
                                               function_name=function_name,
                                               start_time=start_time)
        # Print Results
        print("{} Lambdas Invoked, {} Lambdas completed".format(num_invocations,
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
            pass

        # try the next iteration, (list_objects_v2 only returns first 1000 entries)
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    print("Bucket {} is empty".format(bucket_name))
    return None


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
    log_client = boto3.client('logs', region_name=region_name)
    log_client.delete_log_group(logGroupName='/aws/lambda/{}'.format(function_name))
    log_client.create_log_group(logGroupName='/aws/lambda/{}'.format(function_name))

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


def check_queue(queue_name):
    """
    Args:
        queue_name : queue_name of the queue
    Checks queue for messages, logs queue status of messages left on que and hidden messages
    returns only when queue is empty
    """
    region = get_config()['provider']['region']
    client = boto3.client('sqs', region_name=region)
    logger = logging.getLogger('__main__')

    que_url = client.get_queue_url(QueueName=f"{queue_name}")['QueueUrl']
    response = client.get_queue_attributes(QueueUrl=que_url,
                                           AttributeNames=['ApproximateNumberOfMessages',
                                                           'ApproximateNumberOfMessagesNotVisible'])
    num_messages_on_que = int(response['Attributes']['ApproximateNumberOfMessages'])
    num_messages_hidden = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])

    logger.info(f"{num_messages_on_que} messages left on Que, {num_messages_hidden} messages not visible")

    return num_messages_on_que, num_messages_hidden


def check_dead_letter(queue_name):
    """
    Args:
        queue_name : queue_name of the dead letter queue
    """

    region = get_config()['provider']['region']
    client = boto3.client('sqs', region_name=region)
    logger = logging.getLogger('__main__')

    que_dl_url = client.get_queue_url(QueueName=f"{queue_name}")['QueueUrl']
    response = client.get_queue_attributes(QueueUrl=que_dl_url,
                                           AttributeNames=['ApproximateNumberOfMessages',
                                                           'ApproximateNumberOfMessagesNotVisible'])
    num_dead_letters = int(response['Attributes']['ApproximateNumberOfMessages'])
    if num_dead_letters == 0:
        logger.info("No Dead Letters found. All Que messages successfully processed")
    else:
        logger.info(f"{num_dead_letters} messages failed. Check dead letter que for more info")

    return num_dead_letters


def put_sqs(message_batch, queue_name):
    """
    Args:
        message_batch : list of messages to be sent to the que
        queue_name : name of que to be put on
    """

    region = get_config()['custom']['aws_region']
    client = boto3.client('sqs', region_name=region)
    logger = logging.getLogger('__main__')

    max_batch_size = 10
    num_messages_success = 0
    num_messages_failed = 0
    que_url = client.get_queue_url(QueueName=f"{queue_name}")['QueueUrl']
    logger.info(f"Putting {len(message_batch)} messages onto Que: {que_url}")
    for k in range(0, len(message_batch), max_batch_size):
        response = client.send_message_batch(QueueUrl=que_url,
                                             Entries=message_batch[k:k + max_batch_size])
        num_messages_success += len(response.get('Successful', []))
        num_messages_failed += len(response.get('Failed', []))
    logger.info(f"Total Messages: {len(message_batch)}")
    logger.info(f"Successfully sent: {num_messages_success}")
    logger.info(f"Failed to send: {num_messages_failed}")

    logger.info("Checking SQS Que....")
    while True:
        time.sleep(10)
        response = client.get_queue_attributes(QueueUrl=que_url,
                                               AttributeNames=['ApproximateNumberOfMessages',
                                                               'ApproximateNumberOfMessagesNotVisible'])
        num_messages_on_que = int(response['Attributes']['ApproximateNumberOfMessages'])
        num_messages_hidden = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])

        logger.info(f"{num_messages_on_que} messages left on Que, {num_messages_hidden} messages not visible")
        if num_messages_on_que == 0 and num_messages_hidden == 0:
            break

    return num_messages_success