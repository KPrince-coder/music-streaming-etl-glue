import sys
from datetime import datetime
from typing import List, Dict, Any
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
import boto3
from botocore.exceptions import ClientError
import logging
from decimal import Decimal
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


def read_parquet_safely(spark: SparkSession, path: str, name: str) -> DataFrame:
    """Safely read parquet file with validation"""
    logger.info(f"Reading {name} from: {path}")

    try:
        # For directory-based parquet files, try reading the directory directly first
        base_path = path.split("/part-")[0].strip()
        if base_path.endswith(".parquet"):
            logger.info(f"Attempting to read parquet directory: {base_path}")
            try:
                df = spark.read.parquet(clean_s3_path(base_path))
                record_count = df.count()
                logger.info(
                    f"Successfully read {name} with {record_count} records from directory"
                )
                return df
            except Exception as e:
                logger.warning(f"Failed to read directory directly: {str(e)}")
                # Continue with individual file handling

        # Handle comma-separated paths
        paths = [p.strip() for p in path.split(",")]
        cleaned_paths = []

        for p in paths:
            try:
                cleaned = clean_s3_path(p)
                cleaned_paths.append(cleaned)
            except ValueError as e:
                logger.warning(f"Skipping invalid path {p}: {str(e)}")

        if not cleaned_paths:
            raise FileNotFoundError(f"No valid paths found in: {path}")

        logger.info(f"Attempting to read from paths: {cleaned_paths}")

        # Try reading all paths at once
        df = spark.read.parquet(*cleaned_paths)
        record_count = df.count()
        logger.info(f"Successfully read {name} with {record_count} records")
        return df

    except Exception as e:
        error_msg = f"Failed to read {name}: {str(e)}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg) from e


def clean_s3_path(path: str) -> str:
    """Clean S3 path by removing extra slashes"""
    try:
        # Handle common S3 path format issues
        path = path.strip()

        # Fix malformed s3:/ prefix
        if path.startswith("s3:/") and not path.startswith("s3://"):
            path = "s3://" + path[4:]

        # Add s3:// prefix if missing
        if not path.startswith("s3://"):
            path = "s3://" + path.lstrip("/")

        # Clean the path component
        parts = path.split("://", 1)
        if len(parts) > 1:
            cleaned = f"{parts[0]}://{parts[1].replace('//', '/')}"
            logger.info(f"Cleaned path from '{path}' to '{cleaned}'")
            return cleaned

        cleaned = path.replace("//", "/")
        logger.info(f"Cleaned path from '{path}' to '{cleaned}'")
        return cleaned

    except Exception as e:
        logger.error(f"Error cleaning path '{path}': {str(e)}")
        raise ValueError(f"Invalid S3 path format: {path}") from e


# Get job parameters
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "input_path", "table_name", "kpi_types"]
)


def convert_to_dynamodb_format(item: Dict[str, Any]) -> Dict[str, Any]:
    """Convert item values to DynamoDB-compatible format"""
    converted = {}
    for key, value in item.items():
        if isinstance(value, float):
            converted[key] = Decimal(str(value))
        elif isinstance(value, (int, bool, str)):
            converted[key] = value
        elif value is None:
            continue  # Skip None values
        else:
            converted[key] = str(value)  # Convert other types to string
    return converted


def batch_write_to_dynamodb(items: List[Dict[str, Any]], table_name: str):
    """Write items to DynamoDB table in batches with proper type conversion and retry logic"""
    if not items:
        logger.warning("No items to write to DynamoDB")
        return

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    # Process in batches of 25 (DynamoDB limit)
    batch_size = 25
    processed_count = 0
    max_retries = 5
    base_delay = 1  # Base delay in seconds

    with table.batch_writer(overwrite_by_pkeys=["id", "timestamp"]) as writer:
        for item in items:
            retries = 0
            while True:
                try:
                    # Convert item to DynamoDB format
                    dynamodb_item = convert_to_dynamodb_format(item)
                    writer.put_item(Item=dynamodb_item)
                    processed_count += 1

                    if processed_count % batch_size == 0:
                        logger.info(f"Processed {processed_count} items")
                    break  # Success, exit retry loop

                except ClientError as e:
                    if (
                        e.response["Error"]["Code"]
                        == "ProvisionedThroughputExceededException"
                    ):
                        if retries >= max_retries:
                            logger.error(
                                f"Max retries ({max_retries}) exceeded for item {item.get('id', 'unknown')}"
                            )
                            raise

                        # Calculate exponential backoff delay
                        delay = (2**retries) * base_delay
                        logger.warning(
                            f"Throughput exceeded, retrying in {delay} seconds... (attempt {retries + 1}/{max_retries})"
                        )
                        time.sleep(delay)
                        retries += 1
                    else:
                        logger.error(
                            f"Failed to write item {item.get('id', 'unknown')}: {str(e)}"
                        )
                        logger.error(f"Problematic item: {item}")
                        raise

    logger.info(
        f"Successfully wrote {processed_count} items to DynamoDB table {table_name}"
    )


def process_kpi_data(input_path: str, kpi_types: List[str]) -> tuple:
    """
    Process KPI data from input path and handle null values safely

    Args:
        input_path: Base path for KPI data files
        kpi_types: List of KPI types to process

    Returns:
        Tuple of (user_items, genre_items, trending_items)
    """
    # Read KPI data with logging sample data
    user_df = read_parquet_safely(spark, f"{input_path}/user_kpis", "user KPIs")
    logger.info(f"User KPIs sample data: {user_df.limit(2).toPandas().to_dict()}")

    # Read all three genre KPI types with sample data logging
    daily_metrics_df = read_parquet_safely(
        spark, f"{input_path}/genre_daily_metrics_kpi", "genre daily metrics"
    )
    logger.info(
        f"Daily metrics sample data: {daily_metrics_df.limit(2).toPandas().to_dict()}"
    )

    top_songs_df = read_parquet_safely(
        spark, f"{input_path}/genre_top_songs_kpi", "genre top songs"
    )
    logger.info(f"Top songs sample data: {top_songs_df.limit(2).toPandas().to_dict()}")

    top_genres_df = read_parquet_safely(
        spark, f"{input_path}/genre_top_genres_kpi", "genre top genres"
    )
    logger.info(
        f"Top genres sample data: {top_genres_df.limit(2).toPandas().to_dict()}"
    )

    trending_df = read_parquet_safely(
        spark, f"{input_path}/trending_kpis", "trending KPIs"
    )
    logger.info(
        f"Trending KPIs sample data: {trending_df.limit(2).toPandas().to_dict()}"
    )

    timestamp = datetime.now().isoformat()

    # Process user KPIs with null value handling
    user_items = []
    for row in user_df.collect():
        try:
            # Convert values safely with defaults for null values
            total_time = float(row["total_listening_time_minutes"] or 0.0)
            avg_time = float(row["avg_listening_time_minutes"] or 0.0)
            total_songs = int(row["total_songs_played"] or 0)

            user_items.append(
                {
                    "id": f"USER_{row['user_id']}",
                    "timestamp": timestamp,
                    "kpi_type": "user",
                    "user_id": row["user_id"],
                    "total_songs": total_songs,
                    "total_time": total_time,  # Will be converted to Decimal later
                    "avg_time": avg_time,  # Will be converted to Decimal later
                }
            )
        except (ValueError, TypeError) as e:
            logger.warning(
                f"Skipping invalid user record for user_id {row['user_id']}: {str(e)}"
            )
            continue

    # Process daily genre metrics with null value handling
    daily_genre_items = []
    for row in daily_metrics_df.collect():
        try:
            daily_genre_items.append(
                {
                    "id": f"GENRE_DAILY_{row['track_genre']}_{row['date']}",
                    "timestamp": timestamp,
                    "kpi_type": "genre_daily",
                    "date": row["date"].isoformat(),
                    "genre": row["track_genre"],
                    "listen_count": int(row["listen_count"] or 0),
                    "unique_listeners": int(row["unique_listeners"] or 0),
                    "total_time": float(
                        row["total_listening_time_minutes"] or 0.0
                    ),  # Will be converted to Decimal
                }
            )
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning(
                f"Skipping invalid daily genre record for genre {row['track_genre']}: {str(e)}"
            )
            continue

    # Process top songs per genre with null handling
    top_songs_items = []
    for row in top_songs_df.collect():
        try:
            top_songs_items.append(
                {
                    "id": f"GENRE_TOP_SONGS_{row['track_genre']}_{row['date']}_{row['track_id']}",
                    "timestamp": timestamp,
                    "kpi_type": "genre_top_songs",
                    "date": row["date"].isoformat(),
                    "genre": row["track_genre"],
                    "track_id": row["track_id"],
                    "play_count": int(row["play_count"] or 0),
                    "rank": int(row["rank"] or 0),
                }
            )
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning(
                f"Skipping invalid top songs record for genre {row['track_genre']}, track {row['track_id']}: {str(e)}"
            )
            continue

    # Process top genres with null handling
    top_genres_items = []
    for row in top_genres_df.collect():
        try:
            top_genres_items.append(
                {
                    "id": f"GENRE_TOP_{row['track_genre']}_{row['date']}",
                    "timestamp": timestamp,
                    "kpi_type": "genre_top",
                    "date": row["date"].isoformat(),
                    "genre": row["track_genre"],
                    "listen_count": int(row["listen_count"] or 0),
                    "unique_listeners": int(row["unique_listeners"] or 0),
                    "total_time": float(row["total_listening_time_minutes"] or 0.0),
                    "rank": int(row["rank"] or 0),
                }
            )
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning(
                f"Skipping invalid top genres record for genre {row['track_genre']}: {str(e)}"
            )
            continue

    # Process trending KPIs with null handling
    trending_items = []
    for row in trending_df.collect():
        try:
            trending_items.append(
                {
                    "id": f"TRENDING_{row['track_id']}",
                    "timestamp": timestamp,
                    "kpi_type": "trending",
                    "track_id": row["track_id"],
                    "track_genre": row["track_genre"],
                    "plays_last_24h": int(row["plays_last_24h"] or 0),
                    "total_time": float(row["total_listening_time_minutes"] or 0.0),
                    "unique_listeners": int(row["unique_listeners"] or 0),
                }
            )
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning(
                f"Skipping invalid trending record for track {row['track_id']}: {str(e)}"
            )
            continue

    # Combine all genre items
    genre_items = daily_genre_items + top_songs_items + top_genres_items

    # Log processing statistics
    logger.info("Processed items statistics:")
    logger.info(f"- User KPIs: {len(user_items)} items")
    logger.info(f"- Daily Genre KPIs: {len(daily_genre_items)} items")
    logger.info(f"- Top Songs KPIs: {len(top_songs_items)} items")
    logger.info(f"- Top Genres KPIs: {len(top_genres_items)} items")
    logger.info(f"- Trending KPIs: {len(trending_items)} items")

    # Log sample processed items for verification
    logger.info("Sample processed items:")
    if user_items:
        logger.info(f"User item sample: {user_items[0]}")
    if daily_genre_items:
        logger.info(f"Daily genre item sample: {daily_genre_items[0]}")
    if top_songs_items:
        logger.info(f"Top songs item sample: {top_songs_items[0]}")
    if top_genres_items:
        logger.info(f"Top genres item sample: {top_genres_items[0]}")
    if trending_items:
        logger.info(f"Trending item sample: {trending_items[0]}")

    # Log sample data before conversion
    if user_items:
        logger.info("Sample user item before conversion:", user_items[0])

    # Log sample data after conversion
    if user_items:
        sample_converted = convert_to_dynamodb_format(user_items[0])
        logger.info("Sample user item after conversion:", sample_converted)

    return user_items, genre_items, trending_items


def main():
    """Main execution function"""
    try:
        print(f"Starting DynamoDB load job with parameters: {args}")

        # Parse KPI types
        kpi_types = [t.strip() for t in args["kpi_types"].split(",")]

        # Process KPI data
        user_items, genre_items, trending_items = process_kpi_data(
            args["input_path"], kpi_types
        )

        # Write to DynamoDB
        table_name = args["table_name"]
        print(f"Writing to DynamoDB table: {table_name}")

        batch_write_to_dynamodb(user_items, table_name)
        batch_write_to_dynamodb(genre_items, table_name)
        batch_write_to_dynamodb(trending_items, table_name)

        # Log statistics
        print(f"Successfully loaded to DynamoDB table {table_name}:")
        print(f"- User KPIs: {len(user_items)} items")
        print(f"- Genre KPIs: {len(genre_items)} items")
        print(f"- Trending Songs: {len(trending_items)} items")

        job.commit()

    except Exception as e:
        print(f"Error loading to DynamoDB: {str(e)}")
        raise


if __name__ == "__main__":
    main()
