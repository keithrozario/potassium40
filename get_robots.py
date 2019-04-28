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
                        default=5)
    parser.add_argument("-p", "--per_lambda",
                        help="Number of records to process per lambda, default is 1250",
                        default=10)
    parser.add_argument("-m", "--multiproc_count",
                        help="Number of multi-processes per lambda, default is 125",
                        default=5)

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
    bucket_name = config['custom']['bucketName']
    region = config['custom']['aws_region']
    service_name = config['service']
    queue_name = config['custom']['queueName']
    logger.info(f'Using Serverless deployment {service_name}')
    logger.info(f'Using SQS Queue: {queue_name}')

    # Create Payloads
    for x in range(int(num_invocations)):
        payloads.append({'start_pos': x * per_lambda,
                         'end_pos': (x+1) * per_lambda,
                         'proc_count': proc_count})  # proc_count is the number of processes per lambda
    
    # Package Payloads into SQS Messages
    sqs_messages = [{'MessageBody': json.dumps(payload), 
                     'Id': uuid.uuid4().__str__()} for payload in payloads]

    _start = time.time()
    invocations.put_sqs(sqs_messages, queue_name)

    _end = time.time()
    print("Time Taken to process {:,} urls is {}s".format(total_urls,
                                                          time.time() - _start))

    athena_functions.create_athena_db(bucket_name, region)
    result_file = athena_functions.query_robots(bucket_name, region)

    print("Time Taken to query {:,} urls is {}s".format(total_urls,
                                                        time.time() - _start))

    s3 = boto3.resource('s3')
    result_file_key = result_file.replace(f's3://{bucket_name}/', '')
    logger.info(f'Downloading {result_file_key}')
    s3.Bucket(bucket_name).download_file(result_file_key, result_file_key.split('/')[-1])

    print("Time Taken to download file is {}s".format(time.time() - _start))


