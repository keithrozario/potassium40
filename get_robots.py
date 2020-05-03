#!/usr/bin/env python3

import json
import time
import uuid
import logging
import argparse

import boto3

import invocations
import athena_functions

if __name__ == '__main__':

    # Logging setup
    logging.basicConfig(filename='scan.log',
                        filemode='a',
                        level=logging.INFO,
                        format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logger = logging.getLogger(__name__)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(asctime)s %(message)s', "%H:%M:%S"))
    logger.addHandler(console)

    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--num_invocations",
                        help="Number of lambdas to invoke, default is 100",
                        default=20)
    parser.add_argument("-p", "--per_lambda",
                        help="Number of records to process per lambda, default is 1250",
                        default=2)
    parser.add_argument("-m", "--multiproc_count",
                        help="Number of multi-processes per lambda, default is 125",
                        default=2)

    args = parser.parse_args()

    num_invocations = int(args.num_invocations)
    per_lambda = int(args.per_lambda)
    proc_count = int(args.multiproc_count)
    total_urls = num_invocations * per_lambda

    payloads = []

    # clear the bucket before we start
    logger.info("Clearing bucket before beginning....")
    invocations.clear_bucket()

    # Get Configuration
    config = invocations.get_config()
    bucket_name = invocations.get_bucket_name()
    region = config['custom']['aws_region']
    service_name = config['service']
    queue_names = config['queue_names']
    dl_queue = config['custom']['dlQueueName']
    stage_name = config['custom']['stage']
    logger.info(f'Using Serverless deployment {service_name}')
    logger.info(f'Using SQS Queues: {queue_names}')

    # Create Payloads
    for x in range(int(num_invocations)):
        payloads.append({'start_pos': x * per_lambda,
                         'end_pos': (x+1) * per_lambda,
                         'proc_count': proc_count})  # proc_count is the number of processes per lambda

    # Package Payloads into SQS Messages
    sqs_messages = [{'MessageBody': json.dumps(payload),
                     'Id': uuid.uuid4().__str__()} for payload in payloads]

    _start = time.time()
    invocations.put_sqs(sqs_messages, queue_names)
    invocations.check_dead_letter(dl_queue)
    _end = time.time()
    print("\nTime Taken to process {:,} urls is {}s\n".format(total_urls,
                                                          time.time() - _start))

    # Use Athena to query S3 Bucket
    athena_functions.create_athena_db(bucket_name, region)
    result_file = athena_functions.query_robots(bucket_name, region)
    result_file_key = result_file.replace(f's3://{bucket_name}/', '')
    print("\nTime Taken to query {:,} file is {}s\n".format(len(sqs_messages),
                                                        time.time() - _start))

    # Compress result file
    results = invocations.sync_in_region(function_name=f"{service_name}-{stage_name}-compress_object",
                                         payloads=[{'result_file': result_file_key}])

    result_key = results[0]['resp_payload'].replace(f's3://{bucket_name}/', '')
    logger.info(f'Downloading {result_key}')
    s3 = boto3.resource('s3')
    s3.meta.client.download_file(bucket_name, result_key, result_key)

    print("\nTime Taken to for entire operation: {}s\n".format(time.time() - _start))
