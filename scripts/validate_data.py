"""Data validation script for music streaming pipeline"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import sys
from awsglue.utils import getResolvedOptions
import logging
from functools import reduce
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Update required columns to match actual data structure
REQUIRED_STREAM_COLUMNS = ["user_id", "track_id", "listen_time"]
REQUIRED_SONGS_COLUMNS = [
    "id",
    "track_id",
    "artists",
    "album_name",
    "track_name",
    "popularity",
    "duration_ms",
    "explicit",
    "danceability",
    "energy",
    "key",
    "loudness",
    "mode",
    "speechiness",
    "acousticness",
    "instrumentalness",
    "liveness",
    "valence",
    "tempo",
    "time_signature",
    "track_genre",
]
REQUIRED_USERS_COLUMNS = [
    "user_id",
    "user_name",
    "user_age",
    "user_country",
    "created_at",
]

# Add schema definitions
STREAMS_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("track_id", StringType(), True),
        StructField("listen_time", TimestampType(), True),
    ]
)


def validate_columns(df: DataFrame, required_columns: list, context: str) -> None:
    """Validate that DataFrame has required columns"""
    actual_columns = set(df.columns)
    required_set = set(required_columns)
    missing_columns = required_set - actual_columns

    if missing_columns:
        error_msg = (
            f"Missing required columns in {context}: {list(missing_columns)}\n"
            f"Available columns: {list(actual_columns)}"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)


def check_file_exists(spark, file_path: str) -> bool:
    """Check if file exists in S3"""
    try:
        # Try to get file metadata without reading content
        spark._jsc.hadoopConfiguration().set(
            "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
        )
        return len(spark._jsc.textFile(file_path).take(1)) > 0
    except Exception as e:
        logger.error(f"Error checking file existence {file_path}: {str(e)}")
        return False


def process_streams(spark, stream_files: str, output_path: str) -> DataFrame:
    """Process multiple stream files and union them"""
    logger.info(f"Processing stream files: {stream_files}")

    dfs = []
    for file_path in stream_files.split(","):
        logger.info(f"Processing file: {file_path}")

        if not check_file_exists(spark, file_path):
            logger.error(f"File does not exist: {file_path}")
            continue

        try:
            # Read with explicit schema and options
            df = (
                spark.read.option("header", "true")
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .option("delimiter", ",")
                .option("quote", '"')
                .option("escape", "\\")
                .option("multiLine", "true")
                .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
                .schema(STREAMS_SCHEMA)
                .csv(file_path)
            )

            # Log schema and sample data for debugging
            logger.info(f"File {file_path} schema: {df.schema.simpleString()}")
            sample_data = df.limit(2).collect()
            logger.info(
                f"Sample data from {file_path}: {[row.asDict() for row in sample_data]}"
            )

            # Handle corrupt records
            if "_corrupt_record" in df.columns:
                corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
                if corrupt_count > 0:
                    logger.warning(
                        f"Found {corrupt_count} corrupt records in {file_path}"
                    )
                    df.filter(col("_corrupt_record").isNotNull()).write.mode(
                        "append"
                    ).json(f"{output_path}/corrupt_records/")

            # Validate data
            validate_columns(df, REQUIRED_STREAM_COLUMNS, f"streams file: {file_path}")

            # Clean and select columns
            clean_df = df.select(
                col("user_id").cast("string"),
                col("track_id").cast("string"),
                col("listen_time").cast("timestamp"),
            ).na.drop()  # Remove rows with null values

            dfs.append(clean_df)
            logger.info(f"Successfully processed file: {file_path}")

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}", exc_info=True)
            raise

    if not dfs:
        raise ValueError("No valid stream files were processed")

    # Combine all dataframes
    try:
        result_df = reduce(DataFrame.unionAll, dfs)
        logger.info(f"Total records processed: {result_df.count()}")
        return result_df
    except Exception as e:
        logger.error(f"Error combining dataframes: {str(e)}", exc_info=True)
        raise


def clean_s3_path(path: str) -> str:
    """Clean S3 path by removing extra slashes"""
    # Remove multiple consecutive slashes except after scheme
    parts = path.split("://", 1)
    if len(parts) > 1:
        cleaned = f"{parts[0]}://{parts[1].replace('//', '/')}"
        logger.info(f"Cleaned path from '{path}' to '{cleaned}'")
        return cleaned
    cleaned = path.replace("//", "/")
    logger.info(f"Cleaned path from '{path}' to '{cleaned}'")
    return cleaned


def check_parquet_exists(spark, path: str) -> bool:
    """Check if Parquet files exist at the given path"""
    try:
        # Try listing files at the path
        files = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
        fs = files.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        return fs.exists(files)
    except Exception as e:
        logger.error(f"Error checking Parquet path {path}: {str(e)}")
        return False


def process_reference_data(
    spark, file_path: str, process: bool = False, existing_path: str = None
) -> DataFrame:
    """Process reference data (songs or users) with change detection"""
    logger.info(f"Processing reference data from {file_path}")

    try:
        if not process and existing_path:
            # Clean the path and check if Parquet files exist
            clean_path = clean_s3_path(existing_path)
            logger.info(f"Checking for existing Parquet data at: {clean_path}")

            if check_parquet_exists(spark, clean_path):
                logger.info(f"Reading existing Parquet data from: {clean_path}")
                # Add format('parquet') before read to explicitly specify format
                return spark.read.format("parquet").load(clean_path)
            logger.info(f"No existing Parquet data found at: {clean_path}")

        # Read and process new data
        logger.info(f"Reading new data from: {file_path}")
        new_data = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("multiLine", "true")
            .csv(file_path)
        )

        logger.info(f"Reference data schema: {new_data.schema.simpleString()}")
        logger.info(f"Sample reference data: {new_data.limit(5).toPandas().to_dict()}")

        # Determine data type and validate columns
        if "track_id" in new_data.columns:
            validate_columns(new_data, REQUIRED_SONGS_COLUMNS, "songs")
            # Select and rename relevant columns for songs
            new_data = new_data.select(
                "id",
                "track_id",
                "artists",
                "album_name",
                "track_name",
                "popularity",
                "duration_ms",
                "explicit",
                "danceability",
                "energy",
                "key",
                "loudness",
                "mode",
                "speechiness",
                "acousticness",
                "instrumentalness",
                "liveness",
                "valence",
                "tempo",
                "time_signature",
                "track_genre",
            )
        elif "user_id" in new_data.columns:
            validate_columns(new_data, REQUIRED_USERS_COLUMNS, "users")
            # Select and rename relevant columns for users
            new_data = new_data.select(
                "user_id", "user_name", "user_age", "user_country", "created_at"
            )

        return new_data

    except Exception as e:
        logger.error(f"Error processing reference data {file_path}: {str(e)}")
        raise


def main():
    """Main execution function."""
    try:
        args = getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "input_path",
                "stream_files",
                "songs_file",
                "users_file",
                "output_path",
                "process_songs",
                "process_users",
            ],
        )

        logger.info("Starting data validation job")
        logger.info(f"Job arguments: {args}")

        spark = (
            SparkSession.builder.appName(args["JOB_NAME"])
            .config("spark.sql.debug.maxToStringFields", "100")
            .config("spark.debug.maxToStringFields", "100")
            .getOrCreate()
        )

        # Enable DEBUG logging for Spark
        spark.sparkContext.setLogLevel("DEBUG")

        # Clean output path
        output_path = clean_s3_path(args["output_path"])
        logger.info(f"Using cleaned output path: {output_path}")

        # Process streams with cleaned path
        streams_df = process_streams(spark, args["stream_files"], output_path)
        logger.info(f"Processed {streams_df.count()} stream records")

        # Process reference data with cleaned paths
        songs_df = process_reference_data(
            spark,
            args["songs_file"],
            args["process_songs"].lower() == "true",
            f"{output_path}/songs.parquet",
        )
        logger.info(f"Processed {songs_df.count()} song records")

        users_df = process_reference_data(
            spark,
            args["users_file"],
            args["process_users"].lower() == "true",
            f"{output_path}/users.parquet",
        )
        logger.info(f"Processed {users_df.count()} user records")

        # Save validated data
        logger.info(f"Saving validated data to {output_path}")
        streams_df.write.mode("append").parquet(f"{output_path}/streams.parquet")
        songs_df.write.mode("overwrite").parquet(f"{output_path}/songs.parquet")
        users_df.write.mode("overwrite").parquet(f"{output_path}/users.parquet")

        logger.info("Data validation job completed successfully")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
