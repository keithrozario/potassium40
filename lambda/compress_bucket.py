import os
import gzip
import logging
import tempfile

import boto3


def main(event, context):
    """
    Converts a single file in event['filename'] to gzip compressed into the same bucket
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    file_name = event['result_file']

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(os.environ['bucket_name'])

    tmp = tempfile.SpooledTemporaryFile(mode='w+b')
    logger.info('Downloading result file')
    with tmp as fileobj:
        bucket.download_fileobj(file_name, fileobj)
        fileobj.seek(0)
        with gzip.open(f'/tmp/{file_name}.gz', 'wb') as compressed_file:
            compressed_file.write(fileobj.read())

    s3.Bucket(os.environ['bucket_name']).upload_file(f'/tmp/{file_name}.gz', f'{file_name}.gz')


if __name__ == '__main__':
    main({'result_file': 'temp.txt'}, {})
