# SecLink Data Discovery Pipeline

Author: Yashkumar Patel\
Project Type: Data Engineering / Web Data Ingestion\
Submission For: SecondaryLink Interview Project

------------------------------------------------------------------------

## Project Overview
<img width="1897" height="817" alt="image" src="https://github.com/user-attachments/assets/d0907d29-5914-4e85-9316-991685f639d4" />
<img width="1896" height="791" alt="image" src="https://github.com/user-attachments/assets/8ef46708-d079-45d0-8726-89a6edb9a122" />


This project implements a repeatable data ingestion pipeline that extracts high-signal, explicitly stated information from public company web pages and normalizes it into structured, analytics-ready datasets. The accompanying dashboard provides full pipeline visibility by displaying total runs, successful document fetches, failed fetches, and overall success rate. It also lists failed documents along with the associated error reason for transparency. Successfully processed documents can be selected to view their extracted structured and generic data, enabling traceability from source URL to normalized output.

The pipeline:

-   Ingests a CSV of companies and URLs
-   Fetches public web pages responsibly
-   Extracts structured and semi-structured content
-   Normalizes extracted information into reusable schema formats
-   Produces machine-readable outputs suitable for database loading
-   Provides a frontend dashboard to explore pipeline results

The system is designed using production-oriented data engineering
principles: resilience, traceability, normalization, and
analytics-readiness.

------------------------------------------------------------------------

## Architecture Overview

companies.csv\
↓\
Python Pipeline\
↓\
HTML Fetch + Parsing\
↓\
Fact Extraction + Normalization\
↓\
JSONL Outputs\
↓\
Dashboard (Vanilla JS + Tailwind)

------------------------------------------------------------------------

## Project Structure

data-discovery/\
│\
├── data/\
│ └── companies.csv\
│\
├── python/\
│ └── pipeline.py\
│\
├── out/\
│ ├── companies.jsonl\
│ ├── documents.jsonl\
│ ├── facts.jsonl\
│ └── run_summary.json\
│\
├── index.html\
└── README.md

------------------------------------------------------------------------

## What the Pipeline Extracts

### 1. Document Metadata

-   Source URL\
-   Crawl timestamp\
-   Content type\
-   Company association

### 2. Structured Financial Signals (When Present)

For earnings-related documents: - Revenue statements\
- Operating income (GAAP / Non-GAAP)\
- EPS values\
- Growth percentages\
- Financial guidance references

### 3. Generic High-Signal Facts

-   Numeric statements\
-   Percentage growth\
-   Monetary values\
-   Explicitly stated metrics\
-   Context snippet for traceability

Each extracted fact follows a flexible schema:

{ "company_id": "...", "source_url": "...", "fact_category": "...",
"label": "...", "value_raw": "...", "value_numeric": "...", "unit":
"...", "context_snippet": "..." }

This enables downstream analytics, warehousing, and BI integration.

------------------------------------------------------------------------

## Output Files

companies.jsonl\
Normalized company-level data.

documents.jsonl\
One record per successfully fetched document.

facts.jsonl\
High-signal extracted facts in normalized structure.

run_summary.json\
Pipeline execution metrics including: - Companies processed - Successful
documents - Failed documents - Facts extracted - Completion timestamp

------------------------------------------------------------------------

## How to Run

### 1. Create Virtual Environment

Windows: python -m venv .venv\
.venv`\Scripts`{=tex}`\activate  `{=tex}

Mac/Linux: python3 -m venv .venv\
source .venv/bin/activate

### 2. Install Dependencies

pip install -r python/requirements.txt

### 3. Run Pipeline

python python/pipeline.py --input data/companies.csv --out out

Outputs will be written to the out/ directory.

### 4. Launch Dashboard

Serve locally:

python -m http.server 8000

Open in browser:

http://localhost:8000

------------------------------------------------------------------------

## Design Decisions and Tradeoffs

-   Flexible "facts-style" schema instead of rigid predefined columns\
-   Explicit extraction only (no inference beyond source content)\
-   Error logging without pipeline crashes\
-   JSONL outputs for streaming compatibility\
-   Separation of companies, documents, and facts for scalability

------------------------------------------------------------------------

## Known Limitations

-   Some websites block automated requests\
-   No headless browser rendering implemented\
-   PDF parsing optional and not included\
-   Financial extraction relies on regex heuristics\
-   No distributed crawling or scheduling

------------------------------------------------------------------------

## Potential Improvements

-   Retry with exponential backoff\
-   Rotating user-agents\
-   Headless browser support (Playwright/Selenium)\
-   PDF parsing integration\
-   Database integration (PostgreSQL/Snowflake)\
-   Docker containerization\
-   Airflow orchestration\
-   Incremental crawling support

------------------------------------------------------------------------

## Author

Yashkumar Patel\
AWS & Microsoft Certified Data Engineer\
Google Certified Data Analyst

Built as part of the SecondaryLink Data Discovery interview project.
