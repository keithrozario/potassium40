import boto3
import gzip
import logging
import json
import os


def compress_bucket(event,context):
    """
    compresses all files in an s3_bucket to a zip file
    """

    logger = logging.getLogger()
    level = logging.INFO
    logger.setLevel(level)

    bucket_name = os.environ['bucket_name']
    file_name = 'robots.json.gz'
    file_dir = '/tmp/'  # directory in the

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    downloaded_keys = []
    full_list = []

    logger.info("Reading all files in bucket: {}".format(bucket_name))

    for k, obj in enumerate(bucket.objects.all()):
        if obj.key not in downloaded_keys:
            # Append to downloaded keys, and write contents to io_string
            downloaded_keys.append(obj.key)
            file_list = json.loads((obj.get()['Body'].read()).decode('utf-8'))
            full_list.extend(file_list)

    logger.info("Read in and compressed {:,} files to {}".format(len(downloaded_keys),
                                                                 file_name))
    # write everything to a single file
    with gzip.open(file_dir + file_name, 'wb') as f:
        f.write(json.dumps(full_list).encode('utf-8'))

    # Write IOString to zip file in temp directory
    # Upload zip file back to bucket
    s3.Bucket(bucket_name).upload_file(file_dir + file_name, file_name)


if __name__ == '__main__':
    compress_bucket({}, {})
