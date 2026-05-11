"""
news_pulse.py
-------------
Real-time "News Pulse" pipeline using Spark Structured Streaming.

Reads JSON files arriving in ./stream_input/ (produced by data_generator.py)
and computes four parallel streaming aggregations:

    1. Headline counts grouped by source
    2. Tumbling 1-minute window counts (time-series)
    3. Trending keywords (tokenized, stopword-filtered)
    4. Filtered "breaking news" records

Results are streamed to the console AND written to ./output/ for the
visualize.py step.

Run AFTER starting data_generator.py:
    python news_pulse.py
"""

import os
import shutil
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

STREAM_DIR = "stream_input"
OUTPUT_DIR = "output"
CHECKPOINT_DIR = os.path.join(OUTPUT_DIR, "_checkpoints")

# Very small English stopword list for keyword filtering.
STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "to", "in", "for", "on", "at",
    "by", "with", "as", "is", "are", "was", "were", "be", "been", "being",
    "this", "that", "these", "those", "it", "its", "from", "but", "not",
    "new", "says", "new", "after", "over", "into", "about", "up", "down",
    "breaking", "urgent", "developing", "exclusive",
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("NewsPulse-SE446")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )


def main():
    # Fresh output every run for clean demos.
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(STREAM_DIR, exist_ok=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[news_pulse] Spark version: {spark.version}")

    # Explicit schema (best practice — avoids costly schema inference).
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("source", StringType(), True),
        StructField("category", StringType(), True),
        StructField("headline", StringType(), True),
        StructField("url", StringType(), True),
    ])

    # ------------------------------------------------------------------
    # 1) INGESTION — read JSON files appearing in stream_input/
    # ------------------------------------------------------------------
    raw_stream = (
        spark.readStream
        .schema(schema)
        .option("maxFilesPerTrigger", 5)   # process up to 5 new files per micro-batch
        .json(STREAM_DIR)
    )

    # Tag rows with processing time for windowed agg.
    events = raw_stream.withColumn(
        "event_time", F.coalesce(F.col("timestamp"), F.current_timestamp())
    )

    # ------------------------------------------------------------------
    # 2) AGGREGATION — counts by source
    # ------------------------------------------------------------------
    by_source = (
        events.groupBy("source")
        .agg(F.count("*").alias("headline_count"))
        .orderBy(F.desc("headline_count"))
    )

    q_source = (
        by_source.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .queryName("counts_by_source")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Also persist to memory sink so we can snapshot at the end.
    q_source_mem = (
        by_source.writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("by_source_mem")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # ------------------------------------------------------------------
    # 3) AGGREGATION — tumbling 1-minute window counts (time series)
    # ------------------------------------------------------------------
    windowed = (
        events
        .withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "1 minute"), "category")
        .agg(F.count("*").alias("count"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "category",
            "count",
        )
    )

    q_window = (
        windowed.writeStream
        .outputMode("append")
        .format("memory")
        .queryName("by_window_mem")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ------------------------------------------------------------------
    # 4) AGGREGATION — trending keywords (tokenization)
    # ------------------------------------------------------------------
    stopword_list = list(STOPWORDS)
    stopword_array = F.array(*[F.lit(w) for w in stopword_list])

    keywords = (
        events
        .select(F.lower(F.col("headline")).alias("h"))
        # Strip punctuation, split on whitespace
        .select(F.split(F.regexp_replace("h", r"[^a-z\s]", " "), r"\s+").alias("tokens"))
        .select(F.explode("tokens").alias("word"))
        .where((F.length("word") > 2) & (~F.array_contains(stopword_array, F.col("word"))))
        .groupBy("word")
        .agg(F.count("*").alias("freq"))
        .orderBy(F.desc("freq"))
    )

    q_keywords = (
        keywords.writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("keywords_mem")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ------------------------------------------------------------------
    # 5) FILTER — "breaking" / "urgent" headlines only
    # ------------------------------------------------------------------
    breaking = events.where(
        F.col("headline").rlike("(?i)^(BREAKING|URGENT|DEVELOPING|EXCLUSIVE)")
    ).select("event_time", "source", "category", "headline")

    q_breaking = (
        breaking.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .queryName("breaking_console")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # ------------------------------------------------------------------
    # 6) Periodic snapshots from the in-memory tables to disk so the
    #    visualize.py step has something to read.
    # ------------------------------------------------------------------
    print("[news_pulse] streaming started. Ctrl+C to stop and snapshot results.")
    try:
        import time
        while True:
            time.sleep(10)
            try:
                src_df = spark.sql("SELECT * FROM by_source_mem")
                if src_df.count() > 0:
                    (src_df.coalesce(1).write.mode("overwrite")
                        .option("header", True)
                        .csv(os.path.join(OUTPUT_DIR, "by_source")))
                kw_df = spark.sql(
                    "SELECT * FROM keywords_mem ORDER BY freq DESC LIMIT 30"
                )
                if kw_df.count() > 0:
                    (kw_df.coalesce(1).write.mode("overwrite")
                        .option("header", True)
                        .csv(os.path.join(OUTPUT_DIR, "trending_keywords")))
                win_df = spark.sql("SELECT * FROM by_window_mem")
                if win_df.count() > 0:
                    (win_df.coalesce(1).write.mode("overwrite")
                        .option("header", True)
                        .csv(os.path.join(OUTPUT_DIR, "by_window")))
                print("[news_pulse] snapshot written to ./output/")
            except Exception as e:
                print(f"[news_pulse] snapshot skipped: {e}", file=sys.stderr)
    except KeyboardInterrupt:
        print("\n[news_pulse] stopping queries...")
        for q in [q_source, q_source_mem, q_window, q_keywords, q_breaking]:
            try:
                q.stop()
            except Exception:
                pass
        spark.stop()
        print("[news_pulse] done. Run `python visualize.py` to build the dashboard.")


if __name__ == "__main__":
    main()
