"""
data_generator.py
-----------------
Simulates a continuous stream of news headlines by writing small JSON
files into ./stream_input/ at random intervals. Each file mimics what
an RSS aggregator or news API might emit.

Run in a separate terminal BEFORE starting news_pulse.py:
    python data_generator.py
"""

import json
import os
import random
import time
import uuid
from datetime import datetime

STREAM_DIR = "stream_input"
os.makedirs(STREAM_DIR, exist_ok=True)

SOURCES = [
    "Reuters", "BBC", "CNN", "Al Jazeera", "AP News",
    "The Guardian", "NYTimes", "Bloomberg", "Financial Times", "Sky News",
]

CATEGORIES = ["politics", "tech", "business", "sports", "world", "science", "health"]

# Mock headline fragments — combined randomly to simulate diverse headlines.
SUBJECTS = [
    "Government", "Startup", "Researchers", "President", "Stock market",
    "Climate scientists", "Tech giant", "Central bank", "Olympic athletes",
    "Health officials", "AI model", "Spacecraft", "Election candidate",
    "Energy producer", "Regulator", "Football team", "Diplomat",
]

VERBS = [
    "announces", "warns about", "launches", "investigates", "approves",
    "rejects", "delays", "celebrates", "criticizes", "discovers",
    "unveils", "predicts", "breaks", "achieves", "fails to address",
]

OBJECTS = [
    "new policy", "record results", "global summit", "AI breakthrough",
    "economic downturn", "vaccine rollout", "interest rate change",
    "cybersecurity threat", "renewable energy plan", "trade deal",
    "championship title", "space mission", "data privacy bill",
    "supply chain crisis", "climate agreement", "tax reform",
]

BREAKING_PREFIXES = ["BREAKING:", "URGENT:", "DEVELOPING:", "EXCLUSIVE:", ""]


def make_headline() -> str:
    prefix = random.choices(BREAKING_PREFIXES, weights=[1, 1, 1, 1, 8])[0]
    body = f"{random.choice(SUBJECTS)} {random.choice(VERBS)} {random.choice(OBJECTS)}"
    return f"{prefix} {body}".strip()


def make_record() -> dict:
    return {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": random.choice(SOURCES),
        "category": random.choice(CATEGORIES),
        "headline": make_headline(),
        "url": f"https://example.com/news/{uuid.uuid4().hex[:10]}",
    }


def main():
    print(f"[generator] writing simulated headlines to ./{STREAM_DIR}/")
    print("[generator] press Ctrl+C to stop")
    batch_id = 0
    try:
        while True:
            # Each batch file contains 3-8 headlines.
            batch_size = random.randint(3, 8)
            records = [make_record() for _ in range(batch_size)]

            filename = os.path.join(
                STREAM_DIR, f"batch_{int(time.time()*1000)}_{batch_id}.json"
            )
            # Spark file source reads line-delimited JSON.
            with open(filename, "w", encoding="utf-8") as f:
                for r in records:
                    f.write(json.dumps(r) + "\n")

            print(f"[generator] wrote {batch_size} headlines -> {filename}")
            batch_id += 1
            time.sleep(random.uniform(1.5, 3.0))
    except KeyboardInterrupt:
        print("\n[generator] stopped")


if __name__ == "__main__":
    main()
