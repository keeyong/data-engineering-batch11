from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging

def upload_to_s3(s3_conn_id, s3_bucket, s3_key, local_files_to_upload, replace):
    """
    Upload all of the files to S3
    """
    s3_hook = S3Hook(s3_conn_id)
    dest_key = s3_key
    for file in local_files_to_upload:
        logging.info("Saving {} to {} in S3".format(file, dest_key))
        s3_hook.load_file(
            filename=file,
            key=dest_key,
            bucket_name=s3_bucket,
            replace=replace
        )
