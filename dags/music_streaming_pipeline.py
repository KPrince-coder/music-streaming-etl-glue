"""
Music Streaming ETL Pipeline DAG

This DAG processes music streaming data from S3, transforms it using AWS Glue,
and stores results in DynamoDB for real-time analytics.

The pipeline handles:
1. Data validation
2. KPI computation using Glue
3. DynamoDB ingestion
4. File archival
"""

from datetime import datetime
from typing import Dict
import json
import logging
from contextlib import contextmanager


from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from constants import (
    S3_BUCKET,
    S3_RAW_PREFIX,
    S3_VALIDATED_PREFIX,
    S3_PROCESSED_PREFIX,
    S3_ARCHIVED_PREFIX,
    GLUE_JOB_VALIDATION,
    GLUE_JOB_KPI,
    GLUE_JOB_DYNAMODB,
    GLUE_SCRIPT_VALIDATION,
    GLUE_SCRIPT_KPI,
    GLUE_SCRIPT_DYNAMODB,
    DYNAMODB_TABLE,
    DAG_ID,
    DAG_DESCRIPTION,
    DAG_TAGS,
    DAG_OWNER,
    TASK_RETRIES,
    TASK_RETRY_DELAY,
    TASK_EMAIL_ON_FAILURE,
    SENSOR_POKE_INTERVAL,
    SENSOR_TIMEOUT,
    LOG_FORMAT,
    AWS_CONN_ID,
    AWS_REGION,
)

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def setup_logger():
    """Configure logger with custom format"""
    formatter = logging.Formatter(LOG_FORMAT)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)


@contextmanager
def log_duration():
    """Context manager to log task duration"""
    start_time = datetime.now()
    try:
        yield
    finally:
        duration = datetime.now() - start_time
        logger.info(f"Task duration: {duration.total_seconds():.2f} seconds")


@dag(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    schedule=None,  # Triggered by file arrival
    start_date=days_ago(1),
    catchup=False,
    tags=DAG_TAGS,
    default_args={
        "owner": DAG_OWNER,
        "retries": TASK_RETRIES,
        "retry_delay": TASK_RETRY_DELAY,
        "email_on_failure": TASK_EMAIL_ON_FAILURE,
    },
)
def music_streaming_pipeline():
    """Main DAG for music streaming pipeline"""
    setup_logger()
    logger.info("Initializing music streaming pipeline")

    @task.sensor(
        poke_interval=SENSOR_POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
        mode="poke",  # This ensures the task runs periodically
        soft_fail=True,  # This allows the task to exit gracefully when timeout is reached
    )
    def check_for_data(bucket: str, prefix: str) -> str:
        """
        Check for new data files in S3
        Returns the path of the first matching file
        """
        logger.info(f"Checking for new data in s3://{bucket}/{prefix}")

        with log_duration():
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

            # List all CSV files in the directory
            files = s3_hook.list_keys(
                bucket_name=bucket,
                prefix=prefix,
                delimiter="/",  # This ensures we only get files in the immediate directory
            )

            # Filter for CSV files and sort them
            csv_files = [f for f in files if f and f.lower().endswith(".csv")]
            if not csv_files:
                logger.warning(f"No CSV files found in s3://{bucket}/{prefix}")
                return ""

            # Sort files by name to process them in order
            csv_files.sort()

            # Get the first file that hasn't been processed yet
            for file_key in csv_files:
                # Check if file has already been processed by looking in archived folder
                archived_prefix = f"{S3_ARCHIVED_PREFIX}"
                archived_files = s3_hook.list_keys(
                    bucket_name=bucket, prefix=archived_prefix
                )

                file_name = file_key.split("/")[-1]
                already_processed = any(
                    file_name in archived_key for archived_key in (archived_files or [])
                )

                if not already_processed:
                    found_file = f"s3://{bucket}/{file_key}"
                    logger.info(f"Found new data file to process: {found_file}")
                    return found_file

            logger.info("All existing files have been processed")
            return ""

    @task
    def prepare_validation_job(input_path: str) -> Dict:
        """
        Prepare parameters for Glue validation job
        """
        logger.info(f"Preparing validation job for input: {input_path}")

        job_params = {
            "job_name": GLUE_JOB_VALIDATION,
            "script_location": GLUE_SCRIPT_VALIDATION,
            "script_args": {
                "--input_path": input_path,
                "--output_path": f"s3://{S3_BUCKET}/{S3_VALIDATED_PREFIX}",
            },
        }

        logger.debug(f"Validation job parameters: {json.dumps(job_params, indent=2)}")
        return job_params

    @task
    def prepare_kpi_job(validated_path: str) -> Dict:
        """
        Prepare parameters for KPI computation job
        """
        logger.info(f"Preparing KPI computation job for path: {validated_path}")

        job_params = {
            "job_name": GLUE_JOB_KPI,
            "script_location": GLUE_SCRIPT_KPI,
            "script_args": {
                "--input_path": validated_path,
                "--output_path": f"s3://{S3_BUCKET}/{S3_PROCESSED_PREFIX}",
            },
        }

        logger.debug(f"KPI job parameters: {json.dumps(job_params, indent=2)}")
        return job_params

    @task
    def prepare_dynamodb_job(processed_path: str) -> Dict:
        """
        Prepare parameters for DynamoDB load job
        """
        logger.info(f"Preparing DynamoDB load job for path: {processed_path}")

        job_params = {
            "job_name": GLUE_JOB_DYNAMODB,
            "script_location": GLUE_SCRIPT_DYNAMODB,
            "script_args": {
                "--input_path": processed_path,
                "--table_name": DYNAMODB_TABLE,
            },
        }

        logger.debug(f"DynamoDB job parameters: {json.dumps(job_params, indent=2)}")
        return job_params

    @task
    def archive_files(input_path: str) -> bool:
        """
        Archive processed files to a different S3 location
        """
        if not input_path:
            logger.warning("No input path provided for archival")
            return False

        logger.info(f"Starting file archival process for: {input_path}")

        with log_duration():
            try:
                s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

                # Extract the source key from the full S3 path
                if not input_path.startswith(f"s3://{S3_BUCKET}/"):
                    raise ValueError(f"Invalid input path format: {input_path}")

                source_key = input_path.replace(f"s3://{S3_BUCKET}/", "")

                # Create timestamp-based archive path
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                dest_key = (
                    f"{S3_ARCHIVED_PREFIX}{timestamp}/{source_key.split('/')[-1]}"
                )

                # Ensure archive directory exists
                s3_hook.load_string(
                    string_data="",
                    key=f"{S3_ARCHIVED_PREFIX}.keep",
                    bucket_name=S3_BUCKET,
                    replace=True,
                )

                logger.info(f"Copying file from {source_key} to {dest_key}")
                s3_hook.copy_object(
                    source_bucket_name=S3_BUCKET,
                    dest_bucket_name=S3_BUCKET,
                    source_key=source_key,
                    dest_key=dest_key,
                )

                logger.info(f"Deleting original file: {source_key}")
                s3_hook.delete_objects(bucket_name=S3_BUCKET, keys=[source_key])

                logger.info("File archival completed successfully")
                return True

            except Exception as e:
                logger.error(f"Error during file archival: {str(e)}", exc_info=True)
                raise

    @task
    def run_glue_job(job_params: Dict) -> str:
        """
        Execute a Glue job and return the output path
        """
        job_name = job_params["job_name"]
        logger.info(f"Starting Glue job execution: {job_name}")

        with log_duration():
            try:
                glue_job = GlueJobOperator(
                    task_id=f"glue_job_{job_name}",
                    job_name=job_name,
                    script_location=job_params["script_location"],
                    script_args=job_params["script_args"],
                    aws_conn_id=AWS_CONN_ID,
                    region_name=AWS_REGION,
                    wait_for_completion=True,
                    iam_role_name="AWSGlueServiceRole-MusicStreaming",
                    dag=dag,
                )

                logger.info(f"Executing Glue job: {job_name}")
                glue_job.execute(context={})

                output_path = job_params["script_args"].get("--output_path", "")
                logger.info(
                    f"Glue job {job_name} completed. Output path: {output_path}"
                )
                return output_path

            except Exception as e:
                logger.error(
                    f"Error executing Glue job {job_name}: {str(e)}", exc_info=True
                )
                raise

    # Define the task flow
    logger.info("Setting up pipeline task flow")

    input_file = check_for_data(bucket=S3_BUCKET, prefix=S3_RAW_PREFIX)

    # Only proceed if we have a file to process
    if input_file:
        logger.info(f"Pipeline triggered for input file: {input_file}")

        validation_params = prepare_validation_job(input_file)
        validated_path = run_glue_job(validation_params)
        logger.info(f"Data validation completed. Output: {validated_path}")

        kpi_params = prepare_kpi_job(validated_path)
        processed_path = run_glue_job(kpi_params)
        logger.info(f"KPI computation completed. Output: {processed_path}")

        dynamodb_params = prepare_dynamodb_job(processed_path)
        run_glue_job(dynamodb_params)
        logger.info("DynamoDB load completed")

        archive_files(input_file)
        logger.info("Pipeline execution completed successfully")
    else:
        logger.info("No new files to process. Ending pipeline execution.")


# Create the DAG
dag = music_streaming_pipeline()


# Create the DAG
# dag = music_streaming_pipeline()
