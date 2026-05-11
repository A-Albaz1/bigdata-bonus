"""
visualize.py
------------
Reads CSV snapshots produced by news_pulse.py (in ./output/) and renders
a single static HTML dashboard at ./output/dashboard.html using inline
Chart.js — no external assets needed beyond a CDN at view time.

Run AFTER news_pulse.py has produced at least one snapshot:
    python visualize.py
"""

import glob
import json
import os

import pandas as pd

OUTPUT_DIR = "output"


def load_csv_folder(folder: str) -> pd.DataFrame:
    """Spark writes a directory of part-*.csv files. Concatenate them."""
    path = os.path.join(OUTPUT_DIR, folder)
    if not os.path.isdir(path):
        return pd.DataFrame()
    files = sorted(glob.glob(os.path.join(path, "part-*.csv")))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)


def main():
    by_source = load_csv_folder("by_source")
    keywords = load_csv_folder("trending_keywords")
    by_window = load_csv_folder("by_window")

    if by_source.empty and keywords.empty and by_window.empty:
        print("[visualize] no snapshot data found. Run news_pulse.py first.")
        return

    # Sort & trim for nicer charts.
    if not by_source.empty:
        by_source = by_source.sort_values("headline_count", ascending=False).head(10)
    if not keywords.empty:
        keywords = keywords.sort_values("freq", ascending=False).head(15)
    if not by_window.empty:
        by_window["window_start"] = pd.to_datetime(by_window["window_start"])
        by_window = by_window.sort_values("window_start")

    # Pre-serialize data for embedding in HTML.
    source_labels = by_source["source"].tolist() if not by_source.empty else []
    source_counts = by_source["headline_count"].astype(int).tolist() if not by_source.empty else []

    kw_labels = keywords["word"].tolist() if not keywords.empty else []
    kw_counts = keywords["freq"].astype(int).tolist() if not keywords.empty else []

    # Build a (window_start, category) pivot for time-series.
    if not by_window.empty:
        pivot = (
            by_window.pivot_table(
                index="window_start", columns="category", values="count", aggfunc="sum"
            )
            .fillna(0)
            .sort_index()
        )
        win_labels = [ts.strftime("%H:%M") for ts in pivot.index]
        win_datasets = [
            {"label": str(cat), "data": [int(v) for v in pivot[cat].tolist()]}
            for cat in pivot.columns
        ]
    else:
        win_labels, win_datasets = [], []

    html = """<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<title>News Pulse Dashboard — SE446</title>
<script src=\"https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js\"></script>
<style>
  body { font-family: -apple-system, Segoe UI, Helvetica, Arial, sans-serif;
         margin: 24px; background:#f6f7fb; color:#1f2330; }
  h1 { margin: 0 0 4px 0; }
  .subtitle { color:#666; margin-bottom:24px; }
  .grid { display:grid; grid-template-columns: 1fr 1fr; gap:20px; }
  .card { background:white; border-radius:10px; padding:18px;
          box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
  .full { grid-column: 1 / -1; }
  h2 { font-size:16px; margin:0 0 12px 0; color:#2a2f3a; }
  canvas { max-height: 320px; }
  footer { color:#999; font-size:12px; margin-top:30px; text-align:center; }
</style>
</head>
<body>
  <h1>News Pulse — Streaming Dashboard</h1>
  <div class=\"subtitle\">SE446 Big Data · aggregated from PySpark Structured Streaming output</div>

  <div class=\"grid\">
    <div class=\"card\">
      <h2>Headlines by Source (Top 10)</h2>
      <canvas id=\"sourceChart\"></canvas>
    </div>
    <div class=\"card\">
      <h2>Trending Keywords</h2>
      <canvas id=\"keywordChart\"></canvas>
    </div>
    <div class=\"card full\">
      <h2>Headlines per 1-Minute Window by Category</h2>
      <canvas id=\"windowChart\"></canvas>
    </div>
  </div>

  <footer>Generated locally · refresh dashboard.html after each news_pulse.py snapshot</footer>

<script>
const palette = ['#4f8cff','#ff6b6b','#26c281','#f5a623','#9b59b6','#1abc9c','#e67e22','#34495e'];

new Chart(document.getElementById('sourceChart'), {
  type: 'bar',
  data: {
    labels: __SOURCE_LABELS__,
    datasets: [{ label:'Headlines', data: __SOURCE_COUNTS__, backgroundColor:'#4f8cff' }]
  },
  options: { responsive:true, plugins:{legend:{display:false}} }
});

new Chart(document.getElementById('keywordChart'), {
  type: 'bar',
  data: {
    labels: __KW_LABELS__,
    datasets: [{ label:'Frequency', data: __KW_COUNTS__, backgroundColor:'#26c281' }]
  },
  options: { indexAxis:'y', responsive:true, plugins:{legend:{display:false}} }
});

const winDatasets = __WIN_DATASETS__.map((ds, i) => ({
  ...ds,
  borderColor: palette[i % palette.length],
  backgroundColor: palette[i % palette.length] + '33',
  tension: 0.3, fill: false,
}));

new Chart(document.getElementById('windowChart'), {
  type: 'line',
  data: { labels: __WIN_LABELS__, datasets: winDatasets },
  options: { responsive:true }
});
</script>
</body>
</html>
"""

    html = (
        html
        .replace("__SOURCE_LABELS__", json.dumps(source_labels))
        .replace("__SOURCE_COUNTS__", json.dumps(source_counts))
        .replace("__KW_LABELS__", json.dumps(kw_labels))
        .replace("__KW_COUNTS__", json.dumps(kw_counts))
        .replace("__WIN_LABELS__", json.dumps(win_labels))
        .replace("__WIN_DATASETS__", json.dumps(win_datasets))
    )

    out_path = os.path.join(OUTPUT_DIR, "dashboard.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[visualize] wrote {out_path}")
    print(f"[visualize] sources: {len(source_labels)} | keywords: {len(kw_labels)} | windows: {len(win_labels)}")


if __name__ == "__main__":
    main()
