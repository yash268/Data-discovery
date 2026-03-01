import argparse
import csv
import json
import os
import re
import time
import uuid
from datetime import datetime

import requests
from bs4 import BeautifulSoup


# =========================
# CONFIG
# =========================

REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
RATE_LIMIT_SECONDS = 1

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


# =========================
# UTILS
# =========================

def now():
    return datetime.utcnow().isoformat() + "Z"


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def clean(text):
    if not text:
        return None
    return re.sub(r"\s+", " ", text).strip()


# =========================
# FETCH
# =========================

def fetch(url):
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                return response
        except Exception:
            time.sleep(2 ** attempt)
    return None


# =========================
# PATTERNS
# =========================

KEY_VALUE_PATTERN = re.compile(r"^\s*([A-Za-z0-9 \-/()]+):\s*(.+)$")
MONEY_PATTERN = re.compile(r"\$[\d,\.]+")
PERCENT_PATTERN = re.compile(r"\d+%")
NUMBER_PATTERN = re.compile(r"\b\d[\d,\.]*\b")
DATE_PATTERN = re.compile(
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}, \d{4}\b",
    re.I,
)


# =========================
# SAFE NUMERIC PARSER
# =========================

def safe_parse_float(raw_value):
    """
    Safely extract first valid float from messy string like:
    '$0.68.' or '1,234.56)' or '$12.3B'
    """
    if not raw_value:
        return None

    cleaned = re.sub(r"[^\d.]", "", raw_value)
    match = re.search(r"\d+(\.\d+)?", cleaned)

    if match:
        try:
            return float(match.group())
        except:
            return None

    return None


# =========================
# EXTRACTION LOGIC
# =========================

def extract_facts(company_id, document_id, company_name, url, html):

    soup = BeautifulSoup(html, "lxml")

    facts = []

    def add_fact(category, label=None, value_raw=None,
                 value_numeric=None, unit=None, snippet=None):
        facts.append({
            "fact_id": str(uuid.uuid4()),
            "company_id": company_id,
            "document_id": document_id,
            "company_name": company_name,
            "fact_category": category,
            "label": label,
            "value_raw": value_raw,
            "value_numeric": value_numeric,
            "unit": unit,
            "context_snippet": snippet,
            "source_url": url,
            "parsed_at": now(),
        })

    # ---- Title ----
    if soup.title and soup.title.string:
        title = clean(soup.title.string)
        if title:
            add_fact("metadata", label="title", value_raw=title)

    text_blocks = soup.get_text("\n").split("\n")

    for block in text_blocks:
        block = clean(block)
        if not block:
            continue

        # ---- Key: Value ----
        kv_match = KEY_VALUE_PATTERN.match(block)
        if kv_match:
            label = clean(kv_match.group(1))
            value = clean(kv_match.group(2))
            add_fact(
                category="key_value",
                label=label,
                value_raw=value,
                snippet=block
            )
            continue

        # ---- Money ----
        for money in MONEY_PATTERN.findall(block):
            numeric = safe_parse_float(money)
            if numeric is not None:
                add_fact(
                    category="numeric_statement",
                    value_raw=money,
                    value_numeric=numeric,
                    unit="USD",
                    snippet=block
                )

        # ---- Percent ----
        for percent in PERCENT_PATTERN.findall(block):
            numeric = safe_parse_float(percent)
            if numeric is not None:
                add_fact(
                    category="numeric_statement",
                    value_raw=percent,
                    value_numeric=numeric,
                    unit="percent",
                    snippet=block
                )

        # ---- General Numbers ----
        for number in NUMBER_PATTERN.findall(block):
            numeric = safe_parse_float(number)
            if numeric is not None:
                add_fact(
                    category="numeric_statement",
                    value_raw=number,
                    value_numeric=numeric,
                    unit=None,
                    snippet=block
                )

        # ---- Dates ----
        for date in DATE_PATTERN.findall(block):
            add_fact(
                category="metadata",
                label="date",
                value_raw=date,
                snippet=block
            )

    # ---- Headings ----
    for heading in soup.find_all(["h1", "h2", "h3"]):
        text = clean(heading.get_text())
        if text:
            add_fact(
                category="heading",
                label=heading.name,
                value_raw=text
            )

    return facts


# =========================
# PIPELINE
# =========================

def run_pipeline(input_csv, output_dir):

    os.makedirs(output_dir, exist_ok=True)

    companies = []
    documents = []
    facts = []
    errors = []

    with open(input_csv, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:

            company_name = row.get("Company")
            url = row.get("URL")

            company_id = str(uuid.uuid4())
            document_id = str(uuid.uuid4())

            time.sleep(RATE_LIMIT_SECONDS)

            response = fetch(url)

            if not response:
                errors.append({
                    "company": company_name,
                    "url": url,
                    "error": "fetch_failed"
                })
                continue

            companies.append({
                "company_id": company_id,
                "company_name": company_name,
                "created_at": now(),
            })

            documents.append({
                "document_id": document_id,
                "company_id": company_id,
                "source_url": response.url,
                "crawl_timestamp": now(),
                "content_type": response.headers.get("Content-Type"),
            })

            extracted = extract_facts(
                company_id,
                document_id,
                company_name,
                response.url,
                response.text
            )

            facts.extend(extracted)

    write_jsonl(os.path.join(output_dir, "companies.jsonl"), companies)
    write_jsonl(os.path.join(output_dir, "documents.jsonl"), documents)
    write_jsonl(os.path.join(output_dir, "facts.jsonl"), facts)

    with open(os.path.join(output_dir, "run_summary.json"), "w") as f:
        json.dump({
            "companies_processed": len(companies),
            "documents_processed": len(documents),
            "facts_extracted": len(facts),
            "errors": errors,
            "completed_at": now()
        }, f, indent=2)

    print("Pipeline completed successfully.")


# =========================
# CLI
# =========================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    run_pipeline(args.input, args.out)