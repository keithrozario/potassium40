import json
import yaml
import boto3
import os
import time
import concurrent.futures
import base64
import logging

configuration_file = 'lambda/serverless.yml'
status_file = 'lambda/status.json'
result_folder = 'result'


def get_config():

    with open(configuration_file, 'r') as sls_config:
        config = yaml.load(sls_config.read(), Loader=yaml.Loader)

    # modify Queue Names
    config['queue_names'] = [config['custom'][name] for name in config['custom'].keys() if name.startswith('queue')]

    return config


def get_bucket_name():
    """
    Gets random bucket name from CloudFormation stack
    :return: bucket_name
    """

    config = get_config()
    region = config['custom']['aws_region']
    client = boto3.client('cloudformation', region_name=region)

    stack_name = f"{config['service']}-{config['custom']['stage']}"

    response = client.describe_stack_resources(StackName=stack_name)

    bucket_name = [resource['PhysicalResourceId']
                   for resource in response['StackResources']
                   if resource['LogicalResourceId'] == 'p40Bucket'][0]

    return bucket_name


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


def clear_bucket():
    """ 
    Deletes all objects in Bucket
    use it wisely
    """
    config = get_config()

    region = config['custom']['aws_region']
    bucket_name = get_bucket_name()

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

    bucket_name = get_bucket_name()
    config = get_config()
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
                results.append({'resp_payload': resp_payload[1:-1]})
            else:
                log_result = base64.b64decode(future.result()['LogResult'])
                results.append({'resp_payload': resp_payload[1:-1],
                                'log_result': log_result})

    return results


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

    region = get_config()['custom']['aws_region']
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


def put_sqs(message_batch, queue_names):
    """
    Args:
        message_batch : list of messages to be sent to the que
        queue_names (list) : names of ques to be put on
    """

    region = get_config()['custom']['aws_region']
    client = boto3.client('sqs', region_name=region)
    logger = logging.getLogger('__main__')
    que_urls = get_queue_url(queue_names)

    logger.info(f"Putting {len(message_batch)} messages onto Ques")
    num_messages_success = split_and_put_into_ques(message_batch=message_batch, que_urls=que_urls, client=client)

    logger.info("Checking SQS Que....")

    poll_count = 0
    while True:

        poll_count += 1
        num_messages_on_que = 0
        num_messages_hidden = 0

        for que_url in que_urls:
            response = client.get_queue_attributes(QueueUrl=que_url,
                                                   AttributeNames=['ApproximateNumberOfMessages',
                                                                   'ApproximateNumberOfMessagesNotVisible'])
            num_messages_on_que += int(response['Attributes']['ApproximateNumberOfMessages'])
            num_messages_hidden += int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])

        if num_messages_on_que == 0 and num_messages_hidden == 0:
            logger.info(f"{num_messages_on_que} messages left on Que, {num_messages_hidden} messages not visible")
            break

        if poll_count % 2 == 0:
            logger.info(f"{num_messages_on_que} messages left on Que, {num_messages_hidden} messages not visible")

        time.sleep(2)

    return num_messages_success


def get_queue_url(queue_names: list):
    region = get_config()['custom']['aws_region']
    client = boto3.client('sqs', region_name=region)
    urls = []

    for name in queue_names:
        que_url = client.get_queue_url(QueueName=f"{name}")['QueueUrl']
        urls.append(que_url)

    return urls


def split_and_put_into_ques(message_batch, que_urls, client, max_batch_size=10):
    """
    Args:
        message_batch: List of all messages to be put onto the que
        que_urls: List of que_urls for messages to be put onto
        max_batch_size: Maximum batch size (default to 10 for sqs)
        client: boto3 sqs client
    :return
        num_messages_success: Number of successful messages
    """

    num_messages_success = 0
    num_messages_failed = 0
    logger = logging.getLogger('__main__')

    # Split message batch into equal chunk sizes for each SQS Que
    chunks = [message_batch[i::len(que_urls)] for i in range(len(que_urls))]
    messages_per_que = zip(chunks, que_urls)

    # For each que and chunk, put onto SQS
    for chunk, que_url in messages_per_que:

        for k in range(0, len(chunk), max_batch_size):
            response = client.send_message_batch(QueueUrl=que_url,
                                                 Entries=chunk[k:k + max_batch_size])
            num_messages_success += len(response.get('Successful', []))
            num_messages_failed += len(response.get('Failed', []))
        logger.info(f"Total Successful Messages: {len(chunk)} successful for {que_url}")

    logger.info(f"Total Successful messages for all ques: {num_messages_success}")
    logger.info(f"Total failed messages for all ques: {num_messages_failed}")

    return num_messages_success
