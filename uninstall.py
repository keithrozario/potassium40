import logging
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

    config = invocations.get_config()
    bucket_name = invocations.get_bucket_name()
    region = config['custom']['aws_region']

    logger.info(f"Deleting p40 Athena DB in {region}")
    athena_functions.delete_athena_db(bucket_name=bucket_name, region=region)
    logger.info("Done")

    logger.info(f"Emptying {bucket_name} in {region}")
    invocations.clear_bucket()
