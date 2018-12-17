#!/usr/bin/env python3

import time
import invocations
import json
import boto3

if __name__ == '__main__':

    invocations.clear_bucket()

    payloads = []
    start_pos = 300000
    end_pos = 400000
    per_lambda = 1000
    proc_count = 100

    num_invocations = (end_pos - start_pos) / per_lambda

    for x in range(int(num_invocations)):
        payloads.append({'start_pos': x * per_lambda,
                         'end_pos': (x+1) * per_lambda,
                         'proc_count': proc_count})  # proc_count is the number of Threads per lambda

    _start = time.time()
    results = invocations.async_in_region(function_name='gloda_get_robots',
                                          payloads=payloads,
                                          max_workers=8)

    _end = time.time()
    print("Time Taken to process {:,} urls is {}s".format(end_pos-start_pos,
                                                             _end - _start))

    results = invocations.async_in_region(function_name='gloda_compress_bucket',
                                          payloads=[{}],  # no arguments needed
                                          max_workers=1)

    print("Time Taken to compress {:,} urls is {}s".format(end_pos-start_pos,
                                                           time.time() - _start))

    # download file from S3 bucket
    with open ('lambda/deploy/status.json', 'r') as status_file:
        bucket_name = json.loads(status_file.read()).get('bucket_name', False)

    s3 = boto3.resource('s3')
    result_file = 'robots.json.gz'
    s3.Bucket(bucket_name).download_file(result_file, result_file)

    print("Time Taken to download file is {}s".format(time.time() - _start))
