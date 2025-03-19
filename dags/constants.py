"""
Constants for the music streaming pipeline
"""

from datetime import timedelta

# AWS Configuration
AWS_CONN_ID = "aws_conn"
AWS_REGION = "eu-west-1"

# S3 Configuration
S3_BUCKET = "music-streaming-analyses-bucket"
S3_RAW_PREFIX = "streams/"
S3_VALIDATED_PREFIX = "validated/"
S3_PROCESSED_PREFIX = "processed/"
S3_ARCHIVED_PREFIX = "archived/"
S3_SCRIPTS_PREFIX = "scripts/"

# Glue Job Names
GLUE_JOB_VALIDATION = "music_streaming_validation"
GLUE_JOB_KPI = "music_streaming_kpi_computation"
GLUE_JOB_DYNAMODB = "music_streaming_dynamodb_load"

# Glue Scripts
GLUE_SCRIPT_VALIDATION = f"s3://{S3_BUCKET}/{S3_SCRIPTS_PREFIX}validate_data.py"
GLUE_SCRIPT_KPI = f"s3://{S3_BUCKET}/{S3_SCRIPTS_PREFIX}compute_kpis.py"
GLUE_SCRIPT_DYNAMODB = f"s3://{S3_BUCKET}/{S3_SCRIPTS_PREFIX}load_dynamodb.py"

# Glue Configuration
GLUE_WORKER_TYPE = "G.1X"
GLUE_NUMBER_OF_WORKERS = 2
GLUE_TIMEOUT_MINUTES = 60
GLUE_DEFAULT_ARGUMENTS = {
    "--enable-metrics": "true",
    "--job-language": "python",
    "--TempDir": "s3://aws-glue-temporary-{region}/temporary/",
    "--enable-continuous-cloudwatch-log": "true",
}
GLUE_MAX_CONCURRENT_RUNS = 3
GLUE_MAX_RETRIES = 0

# DynamoDB Configuration
DYNAMODB_TABLE = "music_streaming_kpis"

# DAG Configuration
DAG_ID = "music_streaming_pipeline"
DAG_DESCRIPTION = "Process music streaming data and compute daily KPIs"
DAG_TAGS = ["music", "streaming", "etl"]
DAG_OWNER = "airflow"

# Task Configuration
TASK_RETRIES = 3
TASK_RETRY_DELAY = timedelta(minutes=5)
TASK_EMAIL_ON_FAILURE = False

# Sensor Configuration
SENSOR_POKE_INTERVAL = 300  # 5 minutes
SENSOR_TIMEOUT = 3600  # 1 hour

# Logging Configuration
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
