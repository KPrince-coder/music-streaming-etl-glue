"""Music Streaming Analytics KPI Computation Module"""

import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    count,
    lit,
    sum,
    avg,
    max,
    to_timestamp,
    countDistinct,
    date_trunc,
    dense_rank,
    unix_timestamp,
)
from pyspark.sql import Window
from awsglue.utils import getResolvedOptions
import logging
from urllib.parse import urlparse

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Required arguments for the job
REQUIRED_ARGS = [
    "JOB_NAME",
    "streams_table",
    "songs_table",
    "users_table",
    "output_path",
]

# KPI Configuration
TOP_SONGS_PER_GENRE = 3
TOP_GENRES_PER_DAY = 5
S3_PREFIX = "s3://"


def clean_s3_path(path: str) -> str:
    """Clean S3 path by removing extra slashes and ensuring proper format"""
    if not path:
        return ""

    try:
        # Handle common S3 path format issues
        path = path.strip()

        # Fix malformed s3:/ prefix
        if path.startswith("s3:/") and not path.startswith("s3://"):
            path = S3_PREFIX + path[4:]

        # Add s3:// prefix if missing
        if not path.startswith(S3_PREFIX):
            path = S3_PREFIX + path.lstrip("/")

        # Parse the URL
        parsed = urlparse(path)
        if parsed.scheme != "s3":
            raise ValueError(f"Invalid S3 path scheme: {path}")

        # Clean the path component
        bucket = parsed.netloc
        clean_path = parsed.path.replace("//", "/")
        if clean_path.startswith("/"):
            clean_path = clean_path[1:]

        result = f"s3://{bucket}/{clean_path}"
        logger.debug(f"Cleaned path from '{path}' to '{result}'")
        return result

    except Exception as e:
        logger.error(f"Error cleaning path '{path}': {str(e)}")
        raise ValueError(f"Invalid S3 path format: {path}") from e


def check_path_exists(spark: SparkSession, path: str) -> bool:
    """Check if path exists in S3"""
    try:
        # Handle both directory and file paths
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
        hadoop_fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())

        # Try as directory first
        if hadoop_fs.exists(hadoop_path):
            if hadoop_fs.getFileStatus(hadoop_path).isDirectory():
                # Check if directory contains any files
                files = hadoop_fs.listStatus(hadoop_path)
                return len(files) > 0
            return True

        # Try removing file name and check parent directory
        parent_path = hadoop_path.getParent()
        if hadoop_fs.exists(parent_path):
            files = hadoop_fs.listStatus(parent_path)
            # Check if any file matches our path pattern
            for f in files:
                if f.getPath().toString().startswith(path):
                    return True

        return False
    except Exception as e:
        logger.error(f"Error checking path existence {path}: {str(e)}")
        return False


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


def compute_user_kpis(df: DataFrame) -> DataFrame:
    """
    Compute user-level KPIs.

    Args:
        df: Enriched streaming DataFrame

    Returns:
        DataFrame: User-level KPI metrics
    """
    return (
        df.groupBy("user_id", "user_name", "user_country")
        .agg(
            count("track_id").alias("total_songs_played"),
            sum("listening_time").alias("total_listening_time_minutes"),
            avg("listening_time").alias("avg_listening_time_minutes"),
        )
        .withColumn("kpi_type", lit("user"))
    )


def compute_genre_kpis(df: DataFrame) -> dict:
    """
    Compute genre-level KPIs.

    Args:
        df: Enriched streaming DataFrame

    Returns:
        dict: Genre-level KPI metrics
    """

    daily_df = df.withColumn("date", date_trunc("day", col("timestamp")))

    daily_metrics_kpi = daily_df.groupBy("date", "track_genre").agg(
        count("track_id").alias("listen_count"),
        countDistinct("user_id").alias("unique_listeners"),
        sum("listening_time").alias("total_listening_time_minutes"),
    )

    window_genre = Window.partitionBy("date", "track_genre").orderBy(
        col("play_count").desc()
    )
    top_songs_kpi = (
        daily_df.groupBy("date", "track_genre", "track_id")
        .agg(count("*").alias("play_count"))
        .withColumn("rank", dense_rank().over(window_genre))
        .filter(col("rank") <= TOP_SONGS_PER_GENRE)
    )

    window_day = Window.partitionBy("date").orderBy(col("listen_count").desc())
    top_genres_kpi = daily_metrics_kpi.withColumn(
        "rank", dense_rank().over(window_day)
    ).filter(col("rank") <= TOP_GENRES_PER_DAY)

    return {
        "daily_metrics_kpi": daily_metrics_kpi,
        "top_songs_kpi": top_songs_kpi,
        "top_genres_kpi": top_genres_kpi,
    }


def compute_trending_songs(df: DataFrame) -> DataFrame:
    """
    Compute trending songs based on last 24 hours.

    Args:
        df: Enriched streaming DataFrame

    Returns:
        DataFrame: Trending songs metrics
    """
    # Convert timestamp to Unix timestamp (seconds since epoch)
    df = df.withColumn("unix_timestamp", unix_timestamp(col("timestamp")))

    window_spec = (
        Window.partitionBy("track_id")
        .orderBy(col("unix_timestamp").desc())  # Order by Unix timestamp
        .rangeBetween(-24 * 60 * 60, 0)  # Range in seconds
    )

    return (
        df.withColumn("plays_last_24h", count("track_id").over(window_spec))
        .groupBy("track_id", "track_genre")
        .agg(
            max("plays_last_24h").alias("plays_last_24h"),  # Use pyspark_max here
            sum("listening_time").alias("total_listening_time_minutes"),
            # Use approx_count_distinct or countDistinct directly
            countDistinct("user_id").alias("unique_listeners"),  # Changed this line
        )
        .orderBy(col("plays_last_24h").desc())
        .withColumn("kpi_type", lit("trending"))
    )


def prepare_streaming_data(
    streams_df: DataFrame, songs_df: DataFrame, users_df: DataFrame
) -> DataFrame:
    """Prepare and join streaming data with reference data"""
    logger.info("Preparing streaming data")

    # First, select specific columns from each DataFrame with aliases
    streams_df = streams_df.select(
        col("user_id").alias("stream_user_id"),
        col("track_id").alias("stream_track_id"),
        col("listen_time"),
    ).withColumn("timestamp", to_timestamp(col("listen_time")))

    songs_df = songs_df.select(
        col("track_id").alias("song_track_id"),
        "artists",
        "album_name",
        "track_name",
        "popularity",
        "duration_ms",
        "track_genre",
        # Include other relevant song columns
    )

    users_df = users_df.select(
        col("user_id").alias("user_user_id"),
        "user_name",
        "user_age",
        "user_country",
        "created_at",
    )

    # Perform joins with explicit column references
    enriched_df = streams_df.join(
        songs_df, streams_df.stream_track_id == songs_df.song_track_id, "left"
    ).join(users_df, streams_df.stream_user_id == users_df.user_user_id, "left")

    # Select final columns with clear aliases
    final_df = enriched_df.select(
        col("stream_user_id").alias("user_id"),
        col("user_name"),
        col("user_country"),
        col("stream_track_id").alias("track_id"),
        col("track_name"),
        col("artists"),
        col("album_name"),
        col("track_genre"),
        col("timestamp"),
        (col("duration_ms") / 60000).alias("listening_time"),
    )

    return final_df


def write_parquet_safely(df: DataFrame, path: str, name: str):
    """Safely write parquet file with validation"""
    clean_path = clean_s3_path(path)
    logger.info(f"Writing {name} to: {clean_path}")

    try:
        df.write.mode("overwrite").parquet(clean_path)
        logger.info(f"Successfully wrote {name}")
    except Exception as e:
        raise IOError(f"Failed to write {name} to {clean_path}: {str(e)}") from e


def main():
    """Main execution function"""
    try:
        logger.info("Starting KPI computation job")

        # Get job arguments
        args = getResolvedOptions(sys.argv, REQUIRED_ARGS)
        logger.info(f"Job arguments: {args}")

        # Initialize Spark session
        spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()

        # Read input data with validation
        try:
            streams_df = read_parquet_safely(spark, args["streams_table"], "streams")
        except Exception as e:
            logger.error(f"Failed to read streams data: {str(e)}")
            raise

        songs_df = read_parquet_safely(spark, args["songs_table"], "songs")
        users_df = read_parquet_safely(spark, args["users_table"], "users")

        # Process data
        enriched_df = prepare_streaming_data(streams_df, songs_df, users_df)
        logger.info(f"Processed {enriched_df.count()} streaming records")

        # Compute KPIs
        user_kpis = compute_user_kpis(enriched_df)
        genre_kpis = compute_genre_kpis(enriched_df)
        trending_kpis = compute_trending_songs(enriched_df)

        # Save results
        output_path = clean_s3_path(args["output_path"])
        write_parquet_safely(user_kpis, f"{output_path}/user_kpis", "user KPIs")

        write_parquet_safely(
            trending_kpis, f"{output_path}/trending_kpis", "trending KPIs"
        )

        for kpi_name, df in genre_kpis.items():
            write_parquet_safely(
                df, f"{output_path}/genre_{kpi_name}", f"genre {kpi_name}"
            )
            logger.info(f"Saved genre {kpi_name} KPIs")
            logger.info(f"- Genre {kpi_name}: {df.count()} records")

            logger.info(df.show(5))

        logger.info("KPI computation completed successfully")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
