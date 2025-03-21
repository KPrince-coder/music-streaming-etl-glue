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

from datetime import datetime, timedelta
from typing import Dict
import json
import logging
from contextlib import contextmanager
from botocore.exceptions import ClientError
import time
import random
import boto3

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
        mode="poke",
        soft_fail=False,  # Changed to False to fail the DAG when no files are found
    )
    def check_for_data(bucket: str, prefix: str) -> str:
        """
        Check for new data files in S3
        Returns the path of the first matching file
        """
        logger.info(f"Checking for new data in s3://{bucket}/{prefix}")

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        try:
            files = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)

            if not files:
                logger.info(f"No files found in s3://{bucket}/{prefix}")
                return None  # Return None instead of empty string

            for file_key in files:
                if not file_key.endswith("/"):  # Skip directories
                    already_processed = s3_hook.check_for_key(
                        f"{S3_ARCHIVED_PREFIX}{file_key.split('/')[-1]}",
                        bucket_name=bucket,
                    )

                    if not already_processed:
                        found_file = f"s3://{bucket}/{file_key}"
                        logger.info(f"Found new data file to process: {found_file}")
                        return found_file

            logger.info("All existing files have been processed")
            return None  # Return None instead of empty string

        except Exception as e:
            logger.error(f"Error checking for data: {str(e)}", exc_info=True)
            raise

    @task
    def prepare_validation_job(input_path: str | None) -> Dict | None:
        """
        Prepare parameters for Glue validation job
        Returns None if no input path is provided
        """
        if not input_path:
            logger.info("No input path provided - skipping validation job")
            return None

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
        if not validated_path:
            raise ValueError("No validated path provided for KPI job")

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
        if not processed_path:
            raise ValueError("No processed path provided for DynamoDB job")

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
    def archive_files(input_path: str | None) -> bool:
        """
        Archive processed files to a different S3 location
        """
        if not input_path:
            logger.info("No input path provided - skipping archival")
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

    @task(
        retries=3,  # Increase retries for this specific task
        retry_delay=timedelta(seconds=30),  # Add delay between retries
        retry_exponential_backoff=True,  # Use exponential backoff
        max_retry_delay=timedelta(minutes=10),
    )
    def run_glue_job(job_params: Dict | None) -> str | None:
        """
        Execute a Glue job and return the output path
        Returns None if no job parameters are provided
        """
        if not job_params:
            logger.info("No job parameters provided - skipping Glue job")
            return None

        job_name = job_params["job_name"]
        logger.info(f"Starting Glue job execution: {job_name}")

        with log_duration():
            try:
                task_id = (
                    f"glue_job_{job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                )

                # Initialize Glue client
                glue_client = boto3.client("glue", region_name=AWS_REGION)

                max_retries = 3
                retry_count = 0
                base_delay = 30  # seconds

                while retry_count < max_retries:
                    try:
                        glue_job = GlueJobOperator(
                            task_id=task_id,
                            job_name=job_name,
                            script_location=job_params["script_location"],
                            script_args=job_params["script_args"],
                            aws_conn_id=AWS_CONN_ID,
                            region_name=AWS_REGION,
                            wait_for_completion=True,
                            verbose=True,
                            create_job_kwargs={
                                "GlueVersion": "3.0",
                                "NumberOfWorkers": 2,
                                "WorkerType": "G.1X",
                                "MaxRetries": 0,
                                "Timeout": 60,
                                "MaxConcurrentRuns": 1,
                                "DefaultArguments": {
                                    "--enable-auto-scaling": "true",
                                    "--job-language": "python",
                                    "--continuous-log-logGroup": f"/aws-glue/jobs/{job_name}",
                                    "--enable-glue-datacatalog": "true",
                                },
                            },
                        )

                        glue_job.execute(context={"task": glue_job})
                        break  # If successful, exit the retry loop

                    except ClientError as e:
                        if (
                            e.response["Error"]["Code"]
                            == "ConcurrentRunsExceededException"
                        ):
                            if retry_count < max_retries - 1:
                                delay = (base_delay * (2**retry_count)) + (
                                    random.uniform(0, 10)
                                )
                                logger.warning(
                                    f"Concurrent runs exceeded for job {job_name}. "
                                    f"Attempt {retry_count + 1}/{max_retries}. "
                                    f"Retrying in {delay:.1f} seconds..."
                                )
                                time.sleep(delay)
                                retry_count += 1
                            else:
                                raise RuntimeError(
                                    f"Maximum retries ({max_retries}) reached while waiting "
                                    f"for concurrent jobs to complete. Job: {job_name}"
                                ) from e
                        else:
                            raise

                output_path = job_params["script_args"].get("--output_path", "")
                logger.info(
                    f"Glue job {job_name} completed successfully. Output path: {output_path}"
                )
                return output_path

            except Exception as e:
                logger.error(
                    f"Error executing Glue job {job_name}: {str(e)}", exc_info=True
                )

                # Try to get job run details if available
                try:
                    runs = glue_client.get_job_runs(JobName=job_name, MaxResults=1)
                    if runs["JobRuns"]:
                        last_run = runs["JobRuns"][0]
                        logger.error(
                            f"Last job run details: State={last_run['JobRunState']}, "
                            f"Error={last_run.get('ErrorMessage', 'No error message')}"
                        )
                except Exception as detail_error:
                    logger.error(f"Failed to get job run details: {str(detail_error)}")

                raise RuntimeError(f"Glue job failed: {str(e)}") from e

    # Define the task flow
    logger.info("Setting up pipeline task flow")

    # Define sensor task
    check_data = check_for_data.override(task_id="check_for_data")(
        bucket=S3_BUCKET, prefix=S3_RAW_PREFIX
    )

    # Define validation task group
    validation_params = prepare_validation_job(check_data)
    validation_result = run_glue_job.override(task_id="run_validation_job")(
        validation_params
    )

    # Define KPI computation task group
    kpi_params = prepare_kpi_job(validation_result)
    kpi_result = run_glue_job.override(task_id="run_kpi_job")(kpi_params)

    # Define DynamoDB load task group
    dynamodb_params = prepare_dynamodb_job(kpi_result)
    dynamodb_result = run_glue_job.override(task_id="run_dynamodb_job")(dynamodb_params)

    # Define archive task
    archive_result = archive_files(check_data)

    # Set task dependencies
    check_data >> validation_params
    validation_result >> kpi_params
    kpi_result >> dynamodb_params
    dynamodb_result >> archive_result


# Create the DAG
dag = music_streaming_pipeline()
