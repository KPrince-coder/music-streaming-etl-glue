"""
Music Streaming Analytics KPI Computation Module

This module processes music streaming data to compute various KPIs including:
- User-level metrics
- Genre-level daily metrics
- Trending songs analysis

The module uses PySpark for distributed computing and AWS Glue for ETL operations.
"""

import sys
from typing import Dict, List
from dataclasses import dataclass, field

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import unix_timestamp

from pyspark.sql.functions import (
    col,
    sum,
    count,
    avg,
    lit,
    to_timestamp,
    countDistinct,
    max,
    date_trunc,
    dense_rank,
)

# Type aliases for better readability
KPIResults = Dict[str, DataFrame]


@dataclass
class GenreKPIs:
    """Configuration constants for genre-level KPIs."""

    TOP_SONGS_PER_GENRE: int = 3
    TOP_GENRES_PER_DAY: int = 5


@dataclass
class JobConfig:
    """Configuration for job parameters."""

    REQUIRED_ARGS: List[str] = field(
        default_factory=lambda: ["JOB_NAME", "input_path", "output_path"]
    )


class StreamingDataProcessor:
    """Handles the processing and enrichment of streaming data."""

    def __init__(self, spark_session):
        """
        Initialize the processor with a Spark session.

        Args:
            spark_session: Active Spark session
        """
        self.spark = spark_session

    def prepare_streaming_data(
        self, streams_df: DataFrame, songs_df: DataFrame, users_df: DataFrame
    ) -> DataFrame:
        """
        Prepare and join streaming data with songs and users information.

        Args:
            streams_df: DataFrame containing streaming events
            songs_df: DataFrame containing song metadata
            users_df: DataFrame containing user information

        Returns:
            DataFrame: Enriched streaming data with joined information
        """
        # Convert listen_time to timestamp
        streams_df = streams_df.withColumn(
            "timestamp", to_timestamp(col("listen_time"))
        )

        # Join with songs and users data
        enriched_df = streams_df.join(
            songs_df, streams_df.track_id == songs_df.track_id, "left"
        ).join(users_df, streams_df.user_id == users_df.user_id, "left")

        # Convert duration to minutes
        enriched_df = enriched_df.withColumn(
            "listening_time", col("duration_ms") / 60000
        )

        return enriched_df.select(
            streams_df.user_id,
            streams_df.track_id,
            songs_df.track_genre,
            "listening_time",
            "timestamp",
            users_df.user_name,
            users_df.user_country,
        )


class KPIComputer:
    """Handles computation of various KPIs."""

    @staticmethod
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

    @staticmethod
    def compute_genre_kpis(df: DataFrame) -> KPIResults:
        """
        Compute genre-level KPIs.

        Args:
            df: Enriched streaming DataFrame

        Returns:
            Dict containing different genre KPI DataFrames
        """
        daily_df = df.withColumn("date", date_trunc("day", col("timestamp")))

        # Daily metrics computation
        # Calculate unique listeners first as a subquery
        unique_listeners_df = daily_df.groupBy("date", "track_genre").agg(
            countDistinct("user_id").alias("unique_listeners")
        )

        # Then join with the daily_df and perform other aggregations
        daily_metrics = (
            daily_df.groupBy("date", "track_genre")
            .agg(
                count("track_id").alias("listen_count"),
                sum("listening_time").alias("total_listening_time_minutes"),
            )
            .join(unique_listeners_df, ["date", "track_genre"], "inner")
        )  # Join to get unique_listeners

        # Calculate avg_listening_time_per_user after the join
        daily_metrics = daily_metrics.withColumn(
            "avg_listening_time_per_user",
            col("total_listening_time_minutes") / col("unique_listeners"),
        )

        # Top songs computation
        window_genre_day = Window.partitionBy("date", "track_genre").orderBy(
            col("play_count").desc()
        )

        top_songs = (
            daily_df.groupBy("date", "track_genre", "track_id")
            .agg(count("*").alias("play_count"))
            .withColumn("rank", dense_rank().over(window_genre_day))
            .filter(col("rank") <= GenreKPIs.TOP_SONGS_PER_GENRE)
            .drop("rank")
        )

        # Top genres computation
        window_day = Window.partitionBy("date").orderBy(col("listen_count").desc())

        top_genres = (
            daily_metrics.withColumn("rank", dense_rank().over(window_day))
            .filter(col("rank") <= GenreKPIs.TOP_GENRES_PER_DAY)
            .drop("rank")
        )

        return {
            "daily_metrics": daily_metrics,
            "top_songs": top_songs,
            "top_genres": top_genres,
        }

    @staticmethod
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
                max("plays_last_24h").alias("plays_last_24h"),
                sum("listening_time").alias("total_listening_time_minutes"),
                # Use approx_count_distinct or countDistinct directly
                countDistinct("user_id").alias("unique_listeners"),  # Changed this line
            )
            .orderBy(col("plays_last_24h").desc())
            .withColumn("kpi_type", lit("trending"))
        )


def main():
    """Main execution function."""
    try:
        # Initialize Spark and Glue contexts
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)

        # Get job parameters
        args = getResolvedOptions(sys.argv, JobConfig.REQUIRED_ARGS)
        job.init(args["JOB_NAME"])

        # Initialize processors
        data_processor = StreamingDataProcessor(spark)
        kpi_computer = KPIComputer()

        # Read input data
        input_path = args["input_path"]
        streams_df = spark.read.csv(f"{input_path}/streams1.csv", header=True)
        songs_df = spark.read.csv(f"{input_path}/songs.csv", header=True)
        users_df = spark.read.csv(f"{input_path}/users.csv", header=True)

        # Process data and compute KPIs
        enriched_df = data_processor.prepare_streaming_data(
            streams_df, songs_df, users_df
        )

        user_kpis = kpi_computer.compute_user_kpis(enriched_df)
        genre_kpi_results = kpi_computer.compute_genre_kpis(enriched_df)
        trending_songs = kpi_computer.compute_trending_songs(enriched_df)

        # Write results
        output_path = args["output_path"]
        user_kpis.write.mode("overwrite").parquet(f"{output_path}/user_kpis")

        for kpi_name, kpi_df in genre_kpi_results.items():
            output_location = f"{output_path}/genre_kpis_{kpi_name}"
            kpi_df.write.mode("overwrite").parquet(output_location)
            print(f"- Genre {kpi_name}: {kpi_df.count()} records")

        trending_songs.write.mode("overwrite").parquet(f"{output_path}/trending_songs")

        # Log statistics
        print("Processed KPIs:")
        print(f"- User KPIs: {user_kpis.count()} records")
        print(f"- Trending Songs: {trending_songs.count()} records")

        job.commit()

    except Exception as e:
        print(f"Error in KPI computation: {str(e)}")
        raise


if __name__ == "__main__":
    main()
