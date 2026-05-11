# News Pulse — Real-Time Big Data Pipeline (SE446)

A simplified real-time "News Pulse" big-data pipeline built with **Python** and **PySpark Structured Streaming**. The pipeline ingests simulated news headlines, processes them as a stream, performs aggregations (counts by source, time-windowed trends, keyword frequency, sentiment-style filtering), and outputs both console tables and a static HTML dashboard.

## Project Structure

```
news-pulse/
├── README.md               # This file
├── requirements.txt        # Python dependencies
├── data_generator.py       # Simulates news headlines (writes JSON files to /stream_input)
├── news_pulse.py           # Main PySpark Structured Streaming pipeline
├── visualize.py            # Generates an HTML dashboard from aggregated results
├── run.sh                  # Convenience launcher (Linux/Mac)
├── run.bat                 # Convenience launcher (Windows)
├── sample_headlines.json   # Bootstrap sample data
└── output/                 # Aggregation outputs (created at runtime)
    ├── by_source/
    ├── by_window/
    ├── trending_keywords/
    └── dashboard.html
```

## End-to-End Workflow

1. **Data Ingestion** — `data_generator.py` continuously emits JSON-formatted news headlines (mock RSS-style records) into a watch directory `stream_input/`. Each record has `timestamp`, `source`, `headline`, `category`, and `url`.
2. **Streaming / Incremental Processing** — `news_pulse.py` uses **Spark Structured Streaming** to read the directory as a file stream and processes micro-batches as new files arrive.
3. **Data Processing & Aggregation** — Four parallel streaming queries compute:
   - Headline **counts by source**
   - **Tumbling-window counts** (1-minute windows) for time-series view
   - **Trending keywords** via tokenization + stop-word filtering + word counts
   - **Filtered "breaking news"** records (keyword filter)
4. **Output / Visualization** — Results are written to console (for live monitoring), to Parquet/CSV files under `output/`, and post-aggregated into a lightweight HTML dashboard (`visualize.py`).

## How to Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. In terminal A — start the data generator
python data_generator.py

# 3. In terminal B — start the streaming pipeline
python news_pulse.py

# 4. After collecting a minute or two of data, build the dashboard
python visualize.py
```

Open `output/dashboard.html` in a browser to view final aggregated results.

## What's Demonstrated

- Spark **Structured Streaming** with file source and micro-batch processing
- DataFrame **transformations** (`select`, `filter`, `withColumn`, `explode`, `groupBy`)
- **Window aggregations** over event time with watermarking
- Schema definition, JSON parsing, and tokenization
- Multiple **sinks** (console, memory, file/parquet)
- A complete end-to-end pipeline in under 200 lines of Spark code
