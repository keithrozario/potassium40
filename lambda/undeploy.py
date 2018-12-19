#!/usr/bin/env python3

import boto3
import json
import logging
import os
import yaml

from botocore.exceptions import ClientError

configuration_file = 'lambda/serverless.yml'
status_file = 'lambda/deploy/status.json'
deploy_log = 'lambda/deploy/deploy.log'
default_region = 'us-east-1'


def get_regions():

    with open(status_file, 'r') as f:
        sls_config = json.loads(f.read())

    region = sls_config.get('provider', dict()).get('region', default_region)

    return {'region': region}


def get_config():

    with open(status_file, 'r') as status_reader:
        config = json.loads(status_reader.read())

    return config


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


if __name__ == '__main__':
    # Logging Setup
    logging.basicConfig(filename=deploy_log,
                        filemode='a',
                        level=logging.INFO,
                        format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logger = logging.getLogger(__name__)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logger.addHandler(console)

    with open(status_file, 'r') as f:
        status = json.loads(f.read())

    lambda_regions = [lambda_arn.split(':')[3] for lambda_arn in status.get('lambdas', [])]
    deployed_regions = list(set(lambda_regions))  # make it unique

    for region in deployed_regions:
        lambda_client = boto3.client('lambda', region_name=region)
        logs_client = boto3.client('logs', region_name=region)
        logger.info('INFO: Deleting functions functions in %s' % region)
        for lambda_arn in status.get('lambdas', []):

            if lambda_arn.split(':')[3] == region:
                # Delete lambda and associated log group
                try:
                    lambda_client.delete_function(FunctionName=lambda_arn)
                    function_name = lambda_arn.split(':')[-1]
                    logs_client.delete_log_group(logGroupName='/aws/lambda/{}'.format(function_name))
                    logger.info("Deleted function {} and its logGroup".format(function_name))
                except ClientError:
                    logger.info("ERROR: Unable to delete function")

    if status.get('layer_name', False):

        lambda_client = boto3.client('lambda', region_name=status['region'])
        layer_name = status['layer_name']
        layer_version = int(status['layer_arn'].split(':')[-1:][0])
        response = lambda_client.delete_layer_version(LayerName=layer_name,
                                                      VersionNumber=layer_version)
        logger.info("INFO: Deleted Lambda layer {}, version {}".format(layer_name,
                                                                       layer_version))

    # delete IAM permissions
    if status.get('role_name', False):
        try:
            iam = boto3.client('iam')
            logger.info('INFO: Detaching IAM Policy %s from %s' % (status['policy_arn'], status['role_name']))
            iam.detach_role_policy(RoleName=status['role_name'],
                                   PolicyArn=status['policy_arn'])
            logger.info('INFO: Deleting Role %s' % status['role_name'])
            iam.delete_role(RoleName=status['role_name'])
            logger.info('INFO: Deleting Policy %s' % status['policy_arn'])
            iam.delete_policy(PolicyArn=status['policy_arn'])
        except ClientError:
            pass  # probably deleted it already
    else:
        logger.info("WARNING: No Role found in status, a remnant role may be left in your account!")

    # delete s3 Bucket
    if status.get('bucket_name', False):
        try:
            logger.info("INFO: Deleting items in bucket")
            clear_bucket()
            s3 = boto3.client('s3', region_name=status['region'])
            logger.info('INFO: Deleting S3 Bucket %s' % status['bucket_name'])
            s3.delete_bucket(Bucket=status['bucket_name'])

            # deleting status.json file
            logger.info('INFO: Removing Status File')
            os.remove(status_file)
            logger.info('INFO: Deletion Complete')
        except ClientError:
            pass  # probably deleted it already
    else:
        logger.info("WARNING: No bucket found in status, a remnant bucket may be left in your account!")
