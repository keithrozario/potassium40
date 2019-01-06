#!/usr/bin/env python3

import time
import invocations
import json
import boto3
import argparse

if __name__ == '__main__':

    invocations.clear_bucket()

    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--num_invocations",
                        help="Number of lambdas to invoke, each lambda will process 1250 urls, \
                        set to 800 to process all 1 million",
                        default=100)

    args = parser.parse_args()

    payloads = []
    per_lambda = 1250
    proc_count = 125
    num_invocations = int(args.num_invocations)
    total_urls = num_invocations * per_lambda

    for x in range(int(num_invocations)):
        payloads.append({'start_pos': x * per_lambda,
                         'end_pos': (x+1) * per_lambda,
                         'proc_count': proc_count})  # proc_count is the number of Threads per lambda

    _start = time.time()
    results = invocations.async_in_region(function_name='potassium40-functions-get_robots',
                                          payloads=payloads,
                                          max_workers=4,
                                          sleep_time=5)

    _end = time.time()
    print("Time Taken to process {:,} urls is {}s".format(total_urls,
                                                          time.time() - _start))

    results = invocations.async_in_region(function_name='potassium40-functions-compress_bucket',
                                          payloads=[{}],  # no arguments needed
                                          max_workers=1,
                                          sleep_time=20)

    print("Time Taken to compress {:,} urls is {}s".format(total_urls,
                                                           time.time() - _start))

    with open('status.json', 'r') as status_file:
        bucket_name = json.loads(status_file.read()).get('bucket_name', False)

    s3 = boto3.resource('s3')
    result_file = 'robots.json.gz'
    s3.Bucket(bucket_name).download_file(result_file, result_file)

    print("Time Taken to download file is {}s".format(time.time() - _start))
