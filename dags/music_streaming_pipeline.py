"""
Music Streaming ETL Pipeline DAG

This DAG processes music streaming data from S3, transforms it using AWS Glue,
and stores results in DynamoDB for real-time analytics.
"""

from datetime import datetime
from typing import Any, Dict, List
import logging
import hashlib
import json
import os
import time
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.exceptions import AirflowException
from airflow.models.xcom_arg import XComArg
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

from constants import (
    AWS_CONN_ID,
    AWS_REGION,
    AWS_GLUE_IAM_ROLE,
    GLUE_SCRIPT_DYNAMODB,
    GLUE_WORKER_TYPE,
    GLUE_NUM_WORKERS,
    GLUE_TIMEOUT,
    GLUE_MAX_CONCURRENT_RUNS,
    S3_BUCKET,
    S3_KPIS_PREFIX,
    S3_RAW_PREFIX,
    PROCESSED_FILES_KEY,
    REFERENCE_DATA_STATE_KEY,
    S3_KEY_NOT_FOUND_ERROR,
    S3_VALIDATED_PREFIX,
    S3_SCRIPTS_PREFIX,
    S3_ARCHIVED_PREFIX,
    REQUIRED_S3_DIRS,
    GLUE_SCRIPT_VALIDATION,
    GLUE_SCRIPT_KPI,
    DAG_ID,
    DAG_DESCRIPTION,
    DAG_TAGS,
    DAG_OWNER,
    TASK_RETRIES,
    TASK_RETRY_DELAY,
    TASK_EMAIL_ON_FAILURE,
    GLUE_JOB_VALIDATION,
    GLUE_JOB_KPI,
    GLUE_JOB_DYNAMODB,
)
from scripts.create_dynamodb_table import create_kpi_table

logger = logging.getLogger(__name__)


def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    return logger


logger = setup_logger()


def initialize_s3_structure(s3_hook: S3Hook, bucket: str, prefixes: list[str]) -> None:
    """
    Initialize S3 bucket structure by creating required directories if they don't exist.
    """
    try:
        # Check if bucket exists, create if it doesn't
        if not s3_hook.check_for_bucket(bucket):
            logger.info(f"Creating bucket: {bucket}")
            s3_hook.create_bucket(bucket_name=bucket)

        # Create required prefixes/directories
        for prefix in prefixes:
            # Remove leading/trailing slashes
            clean_prefix = prefix.strip("/")
            if clean_prefix:
                logger.info(f"Ensuring directory exists: s3://{bucket}/{clean_prefix}/")
                # Check if prefix exists with delimiter
                if not s3_hook.check_for_prefix(
                    bucket_name=bucket, prefix=clean_prefix, delimiter="/"
                ):
                    logger.info(f"Created directory: s3://{bucket}/{clean_prefix}/")

    except Exception as e:
        logger.error(f"Failed to initialize S3 structure: {str(e)}")
        raise AirflowException(f"S3 initialization failed: {str(e)}") from e


def initialize_json_files(s3_hook: S3Hook, bucket: str) -> None:
    """Initialize JSON state files if they don't exist"""
    try:
        # Initialize processed files record
        if not s3_hook.check_for_key(key=PROCESSED_FILES_KEY, bucket_name=bucket):
            logger.info(
                f"Creating initial processed files record: {PROCESSED_FILES_KEY}"
            )
            s3_hook.load_string(
                string_data=json.dumps({}), key=PROCESSED_FILES_KEY, bucket_name=bucket
            )

        # Initialize reference data state
        if not s3_hook.check_for_key(key=REFERENCE_DATA_STATE_KEY, bucket_name=bucket):
            logger.info(
                f"Creating initial reference data state: {REFERENCE_DATA_STATE_KEY}"
            )
            initial_state = {
                "songs": "",  # Empty hash indicates no files processed yet
                "users": "",
            }
            s3_hook.load_string(
                string_data=json.dumps(initial_state),
                key=REFERENCE_DATA_STATE_KEY,
                bucket_name=bucket,
            )
    except Exception as e:
        logger.error(f"Failed to initialize JSON files: {str(e)}")
        raise AirflowException(f"JSON files initialization failed: {str(e)}") from e


def get_file_hash(s3_hook: S3Hook, bucket: str, key: str) -> str:
    """Get MD5 hash of file content"""
    content = s3_hook.read_key(key, bucket)
    return hashlib.md5(content.encode(), usedforsecurity=False).hexdigest()


def read_s3_json(s3_hook: S3Hook, bucket: str, key: str) -> dict:
    """Read and parse JSON data from S3, create if doesn't exist"""
    try:
        if not s3_hook.check_for_key(key=key, bucket_name=bucket):
            logger.info(f"Creating new JSON file: {key}")
            initial_data = {}
            if key == REFERENCE_DATA_STATE_KEY:
                initial_data = {"songs": "", "users": ""}
            s3_hook.load_string(
                string_data=json.dumps(initial_data), key=key, bucket_name=bucket
            )
            return initial_data

        content = s3_hook.read_key(key=key, bucket_name=bucket)
        return json.loads(content) if content else {}
    except Exception as e:
        logger.error(f"Error handling JSON file {key}: {str(e)}")
        return {}


@dag(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    # schedule="*/120 * * * *",
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

    @task
    def validate_aws_connection():
        """Validate AWS connection before starting the pipeline"""
        try:
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            # Test connection by listing buckets
            s3_hook.get_conn().list_buckets()
            logger.info("AWS connection validated successfully")
        except Exception as e:
            logger.error(f"AWS connection validation failed: {str(e)}")
            raise AirflowException(f"AWS connection validation failed: {str(e)}") from e

    @task
    def cleanup_s3_scripts():
        """Clean up existing scripts in S3"""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        scripts_to_clean = [
            f"{S3_SCRIPTS_PREFIX}validate_data.py",
            f"{S3_SCRIPTS_PREFIX}compute_kpis.py",
            f"{S3_SCRIPTS_PREFIX}load_dynamodb.py",  # Include this to ensure it's removed
        ]

        for script_key in scripts_to_clean:
            if s3_hook.check_for_key(key=script_key, bucket_name=S3_BUCKET):
                logger.info(f"Deleting existing script: {script_key}")
                s3_hook.delete_objects(bucket=S3_BUCKET, keys=[script_key])

    def upload_script(
        s3_hook: S3Hook, script_name: str, local_path: str, bucket: str
    ) -> bool:
        """Upload a script to S3 and verify its existence"""
        try:
            s3_key = f"{S3_SCRIPTS_PREFIX}{script_name}"
            logger.info(f"Reading local script: {local_path}")

            with open(local_path, "r") as f:
                script_content = f.read()

            logger.info(f"Uploading script to: s3://{bucket}/{s3_key}")
            s3_hook.load_string(
                string_data=script_content,
                key=s3_key,
                bucket_name=bucket,
                replace=True,
            )

            # Verify upload
            if not s3_hook.check_for_key(key=s3_key, bucket_name=bucket):
                logger.error(f"Failed to verify uploaded script: {s3_key}")
                return False

            uploaded_content = s3_hook.read_key(key=s3_key, bucket_name=bucket)
            if uploaded_content.strip() != script_content.strip():
                logger.error(f"Content verification failed for {s3_key}")
                return False

            logger.info(f"Successfully uploaded and verified: {s3_key}")
            return True

        except Exception as e:
            logger.error(f"Error uploading script {script_name}: {str(e)}")
            return False

    @task
    def init_s3_structure():
        """Initialize S3 bucket structure"""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        logger.info(f"Initializing S3 structure in bucket: {S3_BUCKET}")

        initialize_s3_structure(s3_hook, S3_BUCKET, REQUIRED_S3_DIRS)
        initialize_json_files(s3_hook, S3_BUCKET)

        # Upload Glue scripts
        scripts = {
            "validate_data.py": "validate_data.py",
            "compute_kpis.py": "compute_kpis.py",
            "load_dynamodb.py": "load_dynamodb.py",
        }

        upload_failures = []
        for script_name, local_name in scripts.items():
            local_path = f"/usr/local/airflow/scripts/{local_name}"
            if not upload_script(s3_hook, script_name, local_path, S3_BUCKET):
                upload_failures.append(script_name)

        if upload_failures:
            raise AirflowException(f"Failed to upload scripts: {upload_failures}")

    @task
    def verify_glue_scripts():
        """Verify that the correct scripts are in S3"""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        scripts_to_verify = {
            "validate_data.py": "scripts/validate_data.py",
            "compute_kpis.py": "scripts/compute_kpis.py",
            "load_dynamodb.py": "scripts/load_dynamodb.py",
        }

        for script_name, local_path in scripts_to_verify.items():
            s3_key = f"{S3_SCRIPTS_PREFIX}{script_name}"
            full_s3_path = f"s3://{S3_BUCKET}/{s3_key}"

            logger.info(f"Verifying script: {full_s3_path}")
            logger.info(f"Local path: {os.path.abspath(local_path)}")

            if not s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET):
                logger.error(f"Script not found in S3: {full_s3_path}")
                # List contents of scripts directory
                prefix = S3_SCRIPTS_PREFIX.rstrip("/")
                objects = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=prefix)
                logger.info(f"Contents of {prefix}/: {objects}")
                raise ValueError(f"Required script {s3_key} not found in S3")

            # Read both S3 and local content
            s3_content = s3_hook.read_key(key=s3_key, bucket_name=S3_BUCKET)
            with open(local_path, "r") as f:
                local_content = f.read()

            # Compare contents (ignoring whitespace differences)
            if s3_content.strip() != local_content.strip():
                logger.error(f"Content mismatch for {s3_key}")
                logger.error(f"S3 content: {s3_content[:200]}...")
                logger.error(f"Local content: {local_content[:200]}...")
                raise ValueError(f"Content mismatch for {local_path}")

        logger.info("All Glue scripts verified successfully")

    @task
    def check_for_new_streams(bucket: str) -> List[str]:
        """
        Check for new stream files that haven't been processed yet.
        Returns a list of S3 keys for new files.
        """
        try:
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

            # List all files in the raw streams directory
            raw_files = s3_hook.list_keys(
                bucket_name=bucket, prefix=S3_RAW_PREFIX, delimiter="/"
            )

            # Filter out directory marker if present
            raw_files = [f for f in raw_files if f and not f.endswith("/")]

            if not raw_files:
                logger.info("No stream files found in raw directory")
                return []

            # Try to get the processed files record
            try:
                processed_files = read_s3_json(s3_hook, bucket, PROCESSED_FILES_KEY)
            except Exception as e:
                if "Not Found" in str(e) or S3_KEY_NOT_FOUND_ERROR in str(e):
                    logger.info(
                        "No processed files record found - initializing new record"
                    )
                    processed_files = {}
                else:
                    logger.error(f"Error reading processed files record: {str(e)}")
                    raise

            # Find new files by comparing with processed files record
            new_files = []
            for file_key in raw_files:
                if file_key not in processed_files:
                    new_files.append(file_key)

            if new_files:
                logger.info(f"Found {len(new_files)} new stream files: {new_files}")
            else:
                logger.info("No new stream files found")

            return new_files

        except Exception as e:
            logger.error(f"Error checking for new streams: {str(e)}")
            raise AirflowException(f"Failed to check for new streams: {str(e)}") from e

    @task
    def check_reference_data_updates() -> Dict[str, bool]:
        """Check if songs or users data has been updated"""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        updates = {"songs": False, "users": False}

        try:
            # Try to get current state, default to empty dict if file doesn't exist
            current_state = read_s3_json(s3_hook, S3_BUCKET, REFERENCE_DATA_STATE_KEY)

            # Only check files if they exist
            if s3_hook.check_for_key(key="songs/songs.csv", bucket_name=S3_BUCKET):
                songs_hash = get_file_hash(s3_hook, S3_BUCKET, "songs/songs.csv")
                if songs_hash != current_state.get("songs"):
                    updates["songs"] = True
                    current_state["songs"] = songs_hash

            if s3_hook.check_for_key(key="users/users.csv", bucket_name=S3_BUCKET):
                users_hash = get_file_hash(s3_hook, S3_BUCKET, "users/users.csv")
                if users_hash != current_state.get("users"):
                    updates["users"] = True
                    current_state["users"] = users_hash

            # Only update state file if we have changes
            if updates["songs"] or updates["users"]:
                s3_hook.load_string(
                    json.dumps(current_state),
                    REFERENCE_DATA_STATE_KEY,
                    S3_BUCKET,
                    replace=True,
                )

            return updates

        except Exception as e:
            logger.error(f"Error checking reference data updates: {str(e)}")
            # Return default value indicating no updates needed
            return {"songs": False, "users": False}

    @task
    def prepare_data_for_processing(
        stream_files: List[str], ref_updates: Dict[str, bool]
    ) -> Dict[str, Any]:
        """Prepare data processing parameters"""
        if not stream_files:
            logger.info("No stream files to process")
            return None

        # Clean up stream files paths to avoid prefix duplication
        stream_files_paths = []
        for file in stream_files:
            # Remove the prefix if it exists in the file path
            clean_file = file.replace(S3_RAW_PREFIX, "", 1)
            full_path = f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}{clean_file}"
            stream_files_paths.append(full_path)
            logger.info(f"Prepared stream file path: {full_path}")

        # Ensure output path is properly formatted
        output_path = f"s3://{S3_BUCKET}/{S3_VALIDATED_PREFIX}".rstrip("/")
        logger.info(f"Using output path: {output_path}")

        # Prepare job parameters
        job_params = {
            "job_name": GLUE_JOB_VALIDATION,
            "script_location": GLUE_SCRIPT_VALIDATION,
            "script_args": {
                "--JOB_NAME": GLUE_JOB_VALIDATION,
                "--input_path": f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}",
                "--stream_files": ",".join(stream_files_paths),
                "--songs_file": f"s3://{S3_BUCKET}/songs/songs.csv",
                "--users_file": f"s3://{S3_BUCKET}/users/users.csv",
                "--output_path": output_path,
                "--process_songs": str(ref_updates["songs"]).lower(),
                "--process_users": str(ref_updates["users"]).lower(),
            },
        }

        logger.info(f"Prepared job parameters: {json.dumps(job_params, indent=2)}")
        return job_params

    @task
    def update_processed_files(new_files: List[str]) -> bool:
        """
        Update the record of processed files in S3.
        Returns True if update was successful, False otherwise.
        """
        if not new_files:
            logger.info("No new files to record")
            return True

        try:
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

            # Read existing record or initialize new one
            try:
                processed_files = read_s3_json(s3_hook, S3_BUCKET, PROCESSED_FILES_KEY)
            except Exception as e:
                if "Not Found" in str(e) or S3_KEY_NOT_FOUND_ERROR in str(e):
                    processed_files = {}
                else:
                    logger.error(f"Error reading processed files: {str(e)}")
                    return False

            # Update with new files
            timestamp = datetime.now().isoformat()
            for file_key in new_files:
                processed_files[file_key] = {
                    "processed_at": timestamp,
                    "status": "completed",
                }

            # Write back to S3
            try:
                s3_hook.load_string(
                    string_data=json.dumps(processed_files, indent=2),
                    key=PROCESSED_FILES_KEY,
                    bucket_name=S3_BUCKET,
                    replace=True,
                )
            except Exception as e:
                logger.error(f"Failed to write processed files record: {str(e)}")
                return False

            logger.info(
                f"Successfully updated processed files record with {len(new_files)} new files"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to update processed files record: {str(e)}")
            return False

    def clean_s3_path(path: str) -> str:
        """Clean S3 path by removing extra slashes and ensuring proper format"""
        if not path:
            return ""
        # Remove multiple consecutive slashes except after scheme
        parts = path.split("://", 1)
        if len(parts) > 1:
            return f"{parts[0]}://{parts[1].replace('//', '/')}"
        return path.replace("//", "/")

    @task
    def prepare_kpi_job(validated_path: str | None) -> Dict | None:
        """Prepare KPI computation job parameters"""
        if not validated_path:
            return None

        try:
            logger.info(f"Preparing KPI job for validated data: {validated_path}")
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

            # Verify KPI script exists
            script_key = f"{S3_SCRIPTS_PREFIX}compute_kpis.py"
            if not s3_hook.check_for_key(key=script_key, bucket_name=S3_BUCKET):
                logger.error(f"KPI script not found: {script_key}")
                return None

            # Clean paths
            cleaned_validated_path = clean_s3_path(validated_path)
            output_path = clean_s3_path(f"s3://{S3_BUCKET}/{S3_KPIS_PREFIX}")

            # Check for required files
            required_files = {
                "streams.parquet": True,  # True means multiple files are allowed
                "songs.parquet": False,  # False means single file expected
                "users.parquet": False,
            }
            missing_files = []
            file_paths = {}

            for file, allow_multiple in required_files.items():
                file_path = cleaned_validated_path.replace(f"s3://{S3_BUCKET}/", "")
                file_key = f"{file_path}/{file}"

                logger.info(f"Checking for file: {file_key}")

                matching_keys = s3_hook.list_keys(
                    bucket_name=S3_BUCKET, prefix=file_key
                )

                if not matching_keys:
                    missing_files.append(file)
                    logger.warning(f"No files found for: {file_key}")
                    continue

                # Filter out directory entries
                valid_files = [k for k in matching_keys if not k.endswith("/")]

                if allow_multiple:
                    if valid_files:
                        logger.info(
                            f"Found {len(valid_files)} valid files for {file}: {valid_files}"
                        )
                        file_paths[file] = valid_files
                    else:
                        missing_files.append(file)
                        logger.warning(f"No valid files found for {file}")
                else:
                    if len(valid_files) == 1:
                        logger.info(f"Found valid file for {file}: {valid_files[0]}")
                        file_paths[file] = valid_files[0]
                    else:
                        missing_files.append(file)
                        logger.warning(
                            f"Expected single file for {file}, found {len(valid_files)}"
                        )

            if missing_files:
                logger.error(f"Missing or invalid required files: {missing_files}")
                return None

            # For streams, combine all paths with comma
            streams_table = ",".join(
                [f"s3://{S3_BUCKET}/{path}" for path in file_paths["streams.parquet"]]
            )

            # Prepare job parameters
            return {
                "job_name": GLUE_JOB_KPI,
                "script_location": GLUE_SCRIPT_KPI,
                "script_args": {
                    "--input_path": cleaned_validated_path,
                    "--output_path": output_path,
                    "--job_name": GLUE_JOB_KPI,
                    "--streams_table": streams_table,
                    "--songs_table": f"s3://{S3_BUCKET}/{file_paths['songs.parquet']}",
                    "--users_table": f"s3://{S3_BUCKET}/{file_paths['users.parquet']}",
                    "--execution_date": "{{ ds }}",
                },
            }

        except Exception as e:
            logger.error(f"Error preparing KPI job: {str(e)}")
            logger.exception("Full traceback:")
            return None

    @task
    def prepare_dynamodb_job(kpi_path: str | None) -> Dict | None:
        """
        Prepare DynamoDB load job parameters

        Args:
            kpi_path: S3 path containing KPI data

        Returns:
            Dict containing Glue job parameters or None if no data to process
        """
        if not kpi_path:
            logger.info("No KPI path provided - skipping DynamoDB load")
            return None

        try:
            # Verify KPI data exists
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            kpi_prefix = kpi_path.replace(f"s3://{S3_BUCKET}/", "").rstrip("/")

            required_folders = [
                "user_kpis",
                "genre_daily_metrics_kpi",
                "genre_top_songs_kpi",
                "genre_top_genres_kpi",
                "trending_kpis",
            ]
            missing_folders = []

            for folder in required_folders:
                folder_path = f"{kpi_prefix}/{folder}"
                if not s3_hook.check_for_prefix(
                    bucket_name=S3_BUCKET, prefix=folder_path, delimiter="/"
                ):
                    missing_folders.append(folder)

            if missing_folders:
                logger.error(f"Missing KPI folders: {missing_folders}")
                return None

            return {
                "job_name": GLUE_JOB_DYNAMODB,
                "script_location": GLUE_SCRIPT_DYNAMODB,
                "script_args": {
                    "--input_path": kpi_path,
                    "--table_name": "music_streaming_kpis",
                    "--kpi_types": "user,genre,trending",
                },
            }

        except Exception as e:
            logger.error(f"Error preparing DynamoDB job: {str(e)}")
            logger.exception("Full traceback:")
            return None

    @task
    def run_glue_job(job_params: Dict | None) -> str | None:
        """Execute Glue job and return output path"""
        if not job_params:
            logger.info("No job parameters provided - skipping job execution")
            return None

        try:
            job_name = job_params["job_name"]
            script_location = job_params["script_location"]
            script_args = job_params.get("script_args", {})
            output_path = script_args.get("--output_path")

            logger.info(f"Starting Glue job execution: {job_name}")
            logger.info(f"Script location: {script_location}")
            logger.info(f"Script arguments: {json.dumps(script_args, indent=2)}")

            glue_job = GlueJobOperator(
                task_id=f"glue_job_{job_name}",
                job_name=job_name,
                script_location=script_location,
                aws_conn_id=AWS_CONN_ID,
                region_name=AWS_REGION,
                iam_role_name=AWS_GLUE_IAM_ROLE,
                script_args=script_args,
                s3_bucket=S3_BUCKET,
                create_job_kwargs={
                    "GlueVersion": "3.0",
                    "WorkerType": GLUE_WORKER_TYPE,
                    "NumberOfWorkers": GLUE_NUM_WORKERS,
                    "Timeout": GLUE_TIMEOUT,
                    # Removed MaxConcurrentRuns as it's not a valid parameter
                    "ExecutionProperty": {
                        "MaxConcurrentRuns": GLUE_MAX_CONCURRENT_RUNS
                    },
                },
                wait_for_completion=True,
            )

            # Execute the job
            glue_job.execute(context={})
            logger.info(f"Glue job {job_name} completed successfully")

            if not output_path:
                logger.warning("No output path specified in job parameters")
                return None

            # Verify output directory exists
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            max_retries = 5
            retry_delay = 30  # seconds

            for attempt in range(max_retries):
                if s3_hook.check_for_prefix(
                    bucket_name=S3_BUCKET,
                    prefix=output_path.replace(f"s3://{S3_BUCKET}/", "").rstrip("/"),
                    delimiter="/",
                ):
                    logger.info(f"Output verified: {output_path}")
                    return output_path

                if attempt < max_retries - 1:
                    logger.info(
                        f"Output not ready, retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)

            logger.error(
                f"Output not found after {max_retries} attempts: {output_path}"
            )
            return None

        except Exception as e:
            logger.error(f"Failed to execute Glue job {job_name}: {str(e)}")
            logger.exception("Full traceback:")
            raise AirflowException(f"Glue job {job_name} failed: {str(e)}") from e

    @task
    def archive_files(input_paths: List[str] | None) -> bool:
        """
        Archive processed files to dated folders

        Args:
            input_paths: List of S3 paths to archive

        Returns:
            bool: True if archiving successful
        """
        if not input_paths:
            logger.info("No paths to archive")
            return False

        try:
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archived = []

            # List raw files to archive
            raw_files = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_RAW_PREFIX)

            # Filter out directory markers and empty strings
            raw_files = [f for f in raw_files if f and not f.endswith("/")]

            # Add raw files to input paths for archiving
            if raw_files:
                input_paths.append(f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}")
                logger.info(f"Found {len(raw_files)} raw files to archive")

            for input_path in input_paths:
                if not input_path:
                    continue

                # Remove bucket prefix if present
                source_key = input_path.replace(f"s3://{S3_BUCKET}/", "")

                # List all objects under the prefix
                objects = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=source_key)

                if not objects:
                    logger.warning(f"No objects found at path: {source_key}")
                    continue

                for obj_key in objects:
                    if obj_key.endswith("/"):  # Skip directories
                        continue

                    try:
                        # Determine the source directory type (raw, validated, or kpis)
                        if obj_key.startswith(S3_RAW_PREFIX):
                            source_type = "raw"
                        elif obj_key.startswith(S3_VALIDATED_PREFIX):
                            source_type = "validated"
                        elif obj_key.startswith(S3_KPIS_PREFIX):
                            source_type = "kpis"
                        else:
                            source_type = "other"

                        # Create archive path maintaining folder structure
                        relative_path = obj_key.replace(source_key, "").lstrip("/")
                        dest_key = f"{S3_ARCHIVED_PREFIX}{timestamp}/{source_type}/{relative_path}"

                        logger.info(f"Archiving {obj_key} to {dest_key}")

                        # Copy to archive
                        s3_hook.copy_object(
                            source_bucket_name=S3_BUCKET,
                            source_key=obj_key,
                            dest_bucket_name=S3_BUCKET,
                            dest_key=dest_key,
                        )
                        archived.append(obj_key)

                        # Delete original after successful copy
                        s3_hook.delete_objects(bucket=S3_BUCKET, keys=[obj_key])
                        logger.info(f"Successfully archived and deleted: {obj_key}")

                    except Exception as e:
                        logger.error(f"Failed to archive {obj_key}: {str(e)}")
                        raise AirflowException(
                            f"Failed to archive files: {str(e)}"
                        ) from e

            logger.info(
                f"Successfully archived {len(archived)} files to {S3_ARCHIVED_PREFIX}{timestamp}/"
            )
            return True

        except Exception as e:
            logger.error(f"Archive operation failed: {str(e)}")
            raise AirflowException(f"Failed to archive files: {str(e)}") from e

    @task
    def archive_processed_data(
        validated_path: str | None, kpi_path: str | None
    ) -> bool:
        """
        Archive validated and processed data

        Args:
            validated_path: Path to validated data
            kpi_path: Path to KPI data

        Returns:
            bool: True if archiving successful
        """
        # Resolve XCom references if they exist
        if isinstance(validated_path, XComArg):
            validated_path = validated_path.resolve()
        if isinstance(kpi_path, XComArg):
            kpi_path = kpi_path.resolve()

        paths_to_archive = []

        # Add validated data path
        if validated_path:
            paths_to_archive.append(validated_path)

        # Add KPI data path
        if kpi_path:
            paths_to_archive.append(kpi_path)

        # Raw files will be handled within archive_files function

        if not paths_to_archive:
            logger.info("No paths to archive")
            return False

        try:
            logger.info(f"Archiving paths: {paths_to_archive}")
            result = archive_files(paths_to_archive)
            return bool(result)  # Ensure we return a boolean
        except Exception as e:
            logger.error(f"Failed to archive files: {str(e)}")
            return False

    @task
    def ensure_dynamodb_table() -> bool:
        """
        Ensure DynamoDB table exists using existing create_kpi_table function

        Returns:
            bool: True if table exists or was created successfully
        """
        try:
            table_name = "music_streaming_kpis"

            # Get AWS credentials from Airflow connection
            aws_hook = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type="dynamodb")
            credentials = aws_hook.get_credentials()

            logger.info(f"Ensuring DynamoDB table exists: {table_name}")
            create_kpi_table(
                table_name=table_name,
                region=AWS_REGION,
                aws_access_key_id=credentials.access_key,
                aws_secret_access_key=credentials.secret_key,
                aws_session_token=credentials.token,
            )
            return True

        except Exception as e:
            logger.error(f"Failed to ensure DynamoDB table exists: {str(e)}")
            return False

    # Initialize S3 structure first
    validate_conn = validate_aws_connection()
    cleanup_task = cleanup_s3_scripts()
    init_task = init_s3_structure()
    verify_scripts = verify_glue_scripts()
    ensure_table = ensure_dynamodb_table()  # Uses existing create_kpi_table function

    # Set up dependencies for initialization
    validate_conn >> cleanup_task >> init_task >> verify_scripts

    # Check for new data
    new_streams = check_for_new_streams(bucket=S3_BUCKET)
    ref_updates = check_reference_data_updates()

    # Prepare and run validation job
    validation_params = prepare_data_for_processing(new_streams, ref_updates)
    validated_path = run_glue_job(validation_params)

    # Compute KPIs if validation successful
    kpi_params = prepare_kpi_job(validated_path)
    kpi_path = run_glue_job(kpi_params)

    # Load to DynamoDB
    dynamodb_params = prepare_dynamodb_job(kpi_path)
    dynamodb_result = run_glue_job(dynamodb_params)

    # Archive processed data
    archive_result = archive_processed_data(validated_path, kpi_path)

    # Update processed files record
    update_result = update_processed_files(new_streams)

    # Set up the complete task dependencies

    (
        validate_conn
        >> cleanup_task
        >> init_task
        >> verify_scripts
        >> new_streams
        >> ref_updates
        >> validation_params
        >> validated_path
        >> kpi_params
        >> kpi_path
        >> [dynamodb_params, ensure_table]
        >> dynamodb_result
        >> archive_result
        >> update_result
    )


# Create the DAG
dag = music_streaming_pipeline()
