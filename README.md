### Yelp MongoDB Analytics

End-to-end mini data pipeline that fetches restaurant data from the Yelp Fusion API, stores it as nested JSON in MongoDB Atlas, and explores insights about ratings, categories, and pricing across major U.S. cities.

---

### Steps

- **Ingestion**: Python script hits Yelp `/businesses/search` and upserts into MongoDB.
- **Queries**: Reusable MongoDB aggregation pipelines for common analyses.
- **Exploration**: Jupyter notebook visualizes top categories, rating vs. review count, and rating distributions by price.

---

### Prerequisites

- Python 3.10+
- Yelp Fusion API key (create one at [Yelp Developers – Manage App](https://www.yelp.com/developers/v3/manage_app); see [Authentication docs](https://www.yelp.com/developers/documentation/v3/authentication))
- MongoDB Atlas cluster (connection string)

---

### Setup

1) Clone and enter the project

```bash
git clone <this-repo-url>
cd yelp-mongodb-analytics
```

2) Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

3) Install dependencies

```bash
pip install -r requirements.txt
```

4) Register the virtualenv as a Jupyter kernel (optional but recommended)

```bash
python -m ipykernel install --user --name=yelp-mongodb-analytics --display-name "Python (yelp-mongodb-analytics)"
```

5) Configure environment

```bash
cp .env.example .env
# Edit .env to add YELP_API_KEY and MONGODB_URI
```

---

### Quickstart

1) Ingest sample data (50 restaurants per city)

```bash
python -m src.ingest_yelp "New York, NY" "San Francisco, CA" "Los Angeles, CA" "Chicago, IL" "Houston, TX"
```

2) Open the analysis notebook

```bash
jupyter notebook notebooks/analysis.ipynb
```

3) Run the notebook cells to see:

- Top categories by average rating (bar chart)
- Rating vs. review count (scatter)
- Rating distribution by price level (box plot)

---

### Project layout

```
yelp-mongodb-analytics/
├── src/
│   ├── config.py
│   ├── ingest_yelp.py
│   ├── query_mongodb.py
│   └── utils.py
├── notebooks/
│   └── analysis.ipynb
├── data/
├── requirements.txt
├── .env.example
└── README.md
```

---

### Business relevance

This analysis helps identify which restaurant categories perform best in each city, providing insights into customer preferences and market opportunities.

---

### Future work

- Add a Streamlit dashboard to explore categories and cities.
- Implement geospatial aggregation (density by location).
- Export aggregated summaries to CSV under `data/summary/`.
