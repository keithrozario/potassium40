import boto3
import os
import gzip
import tempfile
import logging
import datetime


def main(event, context):
    """
    Converts a single file in event['filename'] to gzip compressed into the same bucket
    gzip is placed into the 'root' directory of the bucket, with the form 'filename'.gz
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    file_name = event['result_file']
    gz_file = f'{file_name.split("/")[-1]}.gz'

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(os.environ['bucket_name'])

    tmp = tempfile.SpooledTemporaryFile(mode='w+b')
    logger.info('Downloading result file')
    with tmp as fileobj:
        bucket.download_fileobj(file_name, fileobj)
        fileobj.seek(0)  # reset stream to beginning of file
        logger.info('Compressing Downloaded File')
        with gzip.open(f'/tmp/{gz_file}', 'wb') as compressed_file:
            compressed_file.write(fileobj.read())
            logger.info('File Compressed, uploading to S3')

    today = datetime.datetime.today()
    output_file = f"robots_{today.year}-{today.month:02}-{today.day:02}.csv.gz"
    s3.Bucket(os.environ['bucket_name']).upload_file(f'/tmp/{gz_file}', output_file)
    os.remove(f'/tmp/{gz_file}')
    logger.info('File Uploaded')

    return f's3://{os.environ["bucket_name"]}/{output_file}'


if __name__ == '__main__':
    main({'result_file': 'temp.txt'}, {})
