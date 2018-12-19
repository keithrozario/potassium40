#!/usr/bin/env python3

import boto3
import uuid
import json
import logging
import yaml
import time
import hashlib
import codecs

from botocore.exceptions import ClientError

status_file = 'lambda/deploy/status.json'
base_policy_file = 'lambda/deploy/base_lambda_policy.json'
layer_directory = 'lambda/layers/'
common_data_layer_file_name = 'common_data.zip'
common_data_lambda_layer_name = 'common_data'
package_zip_file = 'lambda/package/potassium40_package.zip'
package_zip_file = 'lambda/package/potassium40_package.zip'
serverless_yaml = 'lambda/serverless.yml'
deploy_log = 'lambda/deploy/deploy.log'
default_region = 'lambda/us-east-1'


def update_status_file(update):

    try:
        with open(status_file, 'r') as status_reader:
            status = json.loads(status_reader.read())
    except FileNotFoundError:
        status = {}

    for key in update.keys():
        status[key] = update[key]

    with open(status_file, 'w') as status_writer:
        status_writer.write(json.dumps(status, indent=4, sort_keys=True))

    return True


def update_status_field(update):

    """
    Receives a dict of type { "key": "value"}
    creates the dict in the file or appends value to an already existing list of "key" in the status file
    """

    try:
        with open(status_file, 'r') as status_reader:
            status = json.loads(status_reader.read())
    except FileNotFoundError:
        status = {}

    for key in update.keys():
        if key not in status.keys():
            status[key] = []
            status[key].append(update[key])
        else:
            if update[key] not in status[key]:
                status[key].append(update[key])

    with open(status_file, 'w') as status_writer:
        status_writer.write(json.dumps(status, indent=4, sort_keys=True))

    return True


# Create Lambda Policy

def create_role(bucket_arn, base_policy):

    """
    Creates a IAM Role (iam_role_name) and an IAM policy (iam_policy_name)
    attaches the policy to the role.
    Policy allows for role to access bucket objects and list buckets
    Policy does **not** allow for creation/deletion/acl modification
    """

    iam_role_name = 'pottasium40_role'
    iam_policy_name = 'pottasium40_policy'

    # Check if role is already created, skip this step if not required
    try:
        with open(status_file, 'r') as config:
            iam_config = json.loads(config.read())
            role_arn = iam_config['role_arn']
            logger.info("INFO: Found IAM Role, re-using")
            return role_arn

    except (FileNotFoundError, KeyError):

        # not IAM roles found, creating

        iam = boto3.client('iam')

        try:
            response = iam.list_attached_role_policies(RoleName=iam_role_name)
            logger.info("INFO: Found an old version of {}".format(iam_role_name))
            for policy in response.get('AttachedPolicies',[]):
                logger.info("INFO: Detaching {} from {}".format(policy['PolicyName'], iam_role_name))
                iam.detach_role_policy(RoleName=iam_role_name,
                                       PolicyArn=policy['PolicyArn'])
            logger.info("INFO: Deleting {}".format(iam_role_name))
            iam.delete_role(RoleName=iam_role_name)
            time.sleep(3)
            print("INFO: Deleted old role %s, giving it 3 seconds before continuing")
        except ClientError:
            print("INFO: No old roles with name {} found. Great!".format(iam_role_name))

        # Create IAM role with a base policy
        with open(base_policy) as base_policy:
            response = iam.create_role(RoleName=iam_role_name,
                                       AssumeRolePolicyDocument=base_policy.read())
        logger.info('INFO: Created Role for Lambda')
        role_arn = response['Role']['Arn']
        update_status_file({'role_arn': role_arn,
                            'role_name': iam_role_name})

        # Add permission for a s3 bucket
        new_policy = {'Version': "2012-10-17"}
        new_policy['Statement'] = [{"Effect": "Allow",
                                    "Action": [
                                       "s3:PutObject",
                                       "s3:GetObject",
                                       "s3:DeleteObject"
                                       ],
                                    "Resource": bucket_arn},  # bucket_arn is actually bucket objects arn
                                   {"Effect": "Allow",
                                    "Action": "s3:ListBucket",
                                    "Resource": bucket_arn[:-2]},  # list bucket is for bucket (not object)
                                   {
                                    "Effect": "Allow",
                                    "Action": [
                                        "logs:CreateLogStream",
                                        "logs:PutLogEvents"
                                       ],
                                    "Resource": "*"
                                   },
                                   {
                                    "Effect": "Allow",
                                    "Action": "logs:CreateLogGroup",
                                    "Resource": "*"
                                   }]

        # Delete Old policy if it exits

        # if a policy with the same name existed, here's the arn:
        policy_arn = "arn:aws:iam::{}:policy/{}".format(boto3.client('sts').get_caller_identity().get('Account'),
                                                        iam_policy_name)
        try:
            iam.delete_policy(PolicyArn=policy_arn)
            logger.info("INFO: Deleted an old version of {}".format(iam_policy_name))
            logger.info("INFO: Creating new one")
        except ClientError:  # throws exception if policy can't be deleted
            logger.info("INFO: Great! Unable to find policy {} creating a new one".format(iam_policy_name))

        # Create new Policy
        response = iam.create_policy(
            PolicyName=iam_policy_name,
            PolicyDocument=json.dumps(new_policy),
            Description='Additional Policy in addition of base policy'
        )
        logger.info('INFO: Created Policy to access bucket')

        # Attach policy to role
        logger.info("INFO: Attaching policy {} to role {}".format(iam_policy_name,
                                                                  iam_role_name))
        policy_arn = response['Policy']['Arn']
        response = iam.attach_role_policy(
            RoleName=iam_role_name,
            PolicyArn=policy_arn
        )
        logger.info('INFO: Policy Attached to Role')

        # Update status file
        update_status_file({'policy_arn': policy_arn,
                            'policy_name': iam_policy_name})

        # Wait 5 seconds for policy to propogate
        delay = 5
        logger.info("INFO: Waiting {} seconds before Deploying Lambdas".format(delay))
        time.sleep(delay)

        return role_arn


# Create S3 Bucket to upload lambda package
def create_bucket(status_file, region):

    """
    Creates a bucket with the name pottasium40.<xxx> where <xxx> is a uuid version 4
    we use uuid to guarante unique bucket names across aws name space
    if a bucket name already exists in the status.json file, this bucket is used, no new bucket is created
    """

    bucket_prefix = 'pottasium40.'

    # Check if a bucket name exist in status
    try:
        with open(status_file, 'r') as config:
            s3_config = json.loads(config.read())
            bucket_name = s3_config['bucket_name']
            logger.info("INFO: Found S3 Configuration, re-using")
    except (FileNotFoundError, KeyError):
        # Create new bucket
        logger.info("INFO: Unable to find old configuration, creating new bucket")
        s3_client = boto3.client('s3', region_name=region)

        # generate random bucket name
        bucket_name = '{}{}'.format(bucket_prefix, uuid.uuid4())  # use uuid to create random bucket name
        logger.info("INFO: Creating bucket named {}".format(bucket_name))

        # unfortunate inconsistency of boto3, means we have treat us-east-1 special
        if region == 'us-east-1':
            response = s3_client.create_bucket(ACL='private',
                                               Bucket=bucket_name)
        else:
            response = s3_client.create_bucket(ACL='private',
                                               Bucket=bucket_name,
                                               CreateBucketConfiguration={'LocationConstraint': region})
        with open(status_file, 'w') as status:
            status.write(json.dumps({'bucket_name': bucket_name,
                                     'location': response['Location']}))
        logger.info("INFO: New Bucket Created {}".format(bucket_name))

    return bucket_name


def create_lambda(region, role_arn, layer_arns, package):

    """
    Reads the serverless.yml file, and deploys the configuration in a single region
    """

    lambda_client = boto3.client('lambda', region_name=region)

    # Get Lambda config from serverless.yml
    with open(serverless_yaml, 'r') as serverless:
        sls_config = yaml.load(serverless.read())

    # Function Defaults
    runtime = sls_config.get('provider', dict()).get('runtime', 'python3.7')
    provider_timeout = sls_config.get('provider', dict()).get('timeout', 120)
    provider_memorysize = sls_config.get('provider', dict()).get('memorySize', 128)

    # environment variables
    skip_variables= ['lambdas', 'common_data']
    with open(status_file, 'r') as status_reader:
        env_variables = json.loads(status_reader.read())

        # Don't load these variables into env variables
        for var in skip_variables:
            if var in  env_variables.keys():
                del env_variables[var]

    # Create lambdas
    for func in sls_config['functions'].keys():

        logger.info("Creating Lambda Function {}".format(func))

        handler = sls_config.get('functions', dict()).get(func, dict()).get('handler', None)
        desc = sls_config.get('functions', dict()).get(func, dict()).get('description', "No description")
        timeout = sls_config.get('functions', dict()).get(func, dict()).get('timeout', provider_timeout)
        memorysize = sls_config.get('functions', dict()).get(func, dict()).get('memorySize', provider_memorysize)

        try:
            lambda_client.get_function(FunctionName=func)
            # if no exception, we can delete it
            logger.info("INFO: Found old version of function: {} -- deleting".format(func))
            lambda_client.delete_function(FunctionName=func)
            logger.info("INFO: Function deleted")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.debug("INFO: No old lambdas found")

        logger.info("INFO: Creating Lambda Function {}".format(func))
        logger.debug("      Handler: {}".format(handler))
        logger.debug("      Timeout: {}".format(timeout))
        logger.debug("      MemorySize: {}".format(memorysize))

        response = lambda_client.create_function(FunctionName=func,
                                                 Runtime=runtime,
                                                 Role=role_arn,  # passed into function
                                                 Handler=handler,
                                                 Code={'ZipFile': open(package, 'rb').read()},
                                                 Description=desc,
                                                 Timeout=timeout,
                                                 MemorySize=memorysize,
                                                 Publish=True,
                                                 Environment={'Variables': env_variables},
                                                 Layers=layer_arns
                                                 )
        if response.get('FunctionArn', None) is not None:
            logger.info("Lambda Function Created: {}".format(response['FunctionArn']))
            update_status_field({"lambdas": response['FunctionArn']})
        else:
            logger.info("Error Creating Lambda")

    return True


def publish_layer(region, bucket_name, file_name, layer_name, layer_desc='no description', layer_license='MIT'):

    """
    Publish a single layer configuration to this region
    """

    lambda_client = boto3.client('lambda', region_name=region)

    # check for lambda layer
    try:
        # Get SHA256 hash of latest version of layer, throws exception if none found
        response = lambda_client.list_layer_versions(LayerName=layer_name)['LayerVersions'][0]
        layer_arn = response['LayerVersionArn']
        response = lambda_client.get_layer_version(LayerName=layer_name,
                                                   VersionNumber=response['Version'])
        layer_CodeSha256 = response['Content']['CodeSha256']

        # Calculate hash of current layer
        with open(layer_directory + file_name, 'rb') as layer:
            file_hash = hashlib.sha256(layer.read()).hexdigest()
            b64_hash = codecs.encode(codecs.decode(file_hash, 'hex'), 'base64').decode().strip()

        # Compare with hash of what's in AWS already
        if b64_hash == layer_CodeSha256:
            logger.info('INFO: No new changes to layer, skipping publishing')
            deploy = False
        else:
            logger.info('INFO: Changes detected in layer, publishing new version')
            deploy = True

    except (FileNotFoundError, KeyError, IndexError):
        logger.info("INFO: No layer info found, publishing new version")
        deploy = True

    if deploy:

        s3 = boto3.resource('s3', region_name=region)
        s3_client = boto3.client('s3', region_name=region)

        lambda_layer_zip = layer_directory + file_name
        # Upload file to S3 bucket
        s3.meta.client.upload_file(lambda_layer_zip, bucket_name, file_name)

        # Publish Layer and make it accessible
        response = lambda_client.publish_layer_version(
            LayerName=layer_name,
            Description=layer_desc,
            Content={'S3Bucket': bucket_name,
                     'S3Key': file_name},
            CompatibleRuntimes=['python3.6'],
            LicenseInfo=layer_license)

        # layer arn must include the version
        layer_arn = response['LayerVersionArn']
        layer_CodeSha256 = response['Content']['CodeSha256']

        # Delete zip file that was uploaded
        s3_client.delete_object(Bucket=bucket_name,
                                Key=file_name)

        # Modify permissions to readable to your account only
        response = lambda_client.add_layer_version_permission(
            LayerName=layer_name,
            VersionNumber=response['Version'],
            StatementId='only_this_account',
            Action='lambda:GetLayerVersion',
            Principal=boto3.client('sts').get_caller_identity().get('Account')  # change to * to make public
        )

    return {'arn': layer_arn,
            'codeSha256': layer_CodeSha256}


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

    logger.info("INFO: Starting...")

    # Load settings from serverless.yml
    with open(serverless_yaml, 'r') as serverless:
        sls_config = yaml.load(serverless.read())

    region = sls_config.get('provider', dict()).get('region', default_region)
    logger.info("INFO: Region {} selected".format(region))

    # Create Bucket
    bucket_name = create_bucket(status_file=status_file,
                                region=region)
    bucket_arn = 'arn:aws:s3:::{}/*'.format(bucket_name)
    update_status_file({'bucket_arn': bucket_arn,
                        'bucket_name': bucket_name,
                        'region': region})

    # Create Role & Policy
    role_arn = create_role(bucket_arn=bucket_arn,
                           base_policy=base_policy_file)

    # Deploy common data Lambda Layer
    layer_arns = []

    response = publish_layer(region=region,
                             bucket_name=bucket_name,
                             file_name=common_data_layer_file_name,
                             layer_name=common_data_lambda_layer_name,
                             layer_desc='Common Data files for all lambdas')
    layer_arns.append(response['arn'])

    update_status_file({'layer_name': common_data_lambda_layer_name,
                        'layer_arn': response['arn'],
                        'layer_codeSha256': response['codeSha256']})

    # Deploy Lambdas
    create_lambda(region=region,
                  role_arn=role_arn,
                  layer_arns=layer_arns,
                  package=package_zip_file)
