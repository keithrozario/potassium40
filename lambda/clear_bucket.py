import boto3
import os
import logging

logger = logging.getLogger()
level = logging.INFO
logger.setLevel(level)


def clear_bucket(event,context):
    """
    compresses all files in an s3_bucket to a zip file
    """
    logger.info('__start__')
    s3_client = boto3.client('s3')

    kwargs = {'Bucket': os.environ['bucket_name']}

    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        keys = []

        for obj in resp.get('Contents', []):
            keys.append({'Key': obj['Key']})

        if len(keys) > 0:
            s3_client.delete_objects(Bucket=os.environ['bucket_name'],
                                     Delete={'Objects': keys})
        else:
            print("Bucket is empty.")

        # try the next iteration, (list_objects_v2 only returns first 1000 entries)
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
    logger.info('__end__')
    return keys
