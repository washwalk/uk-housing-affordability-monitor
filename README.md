# UK Housing Affordability Monitor

Automated Quarto and Python dashboard using ONS API data to monitor UK housing affordability trends, published via GitHub Pages.

## Overview

This repository demonstrates a reproducible statistical publication workflow using official ONS data, Python data processing, Quarto reporting, GitHub Actions automation, and GitHub Pages publishing.

The monitor calculates a clear headline affordability indicator:

```text
median house price / median gross annual resident earnings
```

The current implementation focuses on England and Wales because the ONS small-area house price dataset used by the project covers England and Wales.

## Data Sources

The pipeline queries the ONS API for the latest published versions of:

- `house-prices-local-authority`: House price statistics for small areas in England and Wales
- `ashe-tables-7-and-8`: Earnings and hours worked, place of work and residence by local authority

It then streams the CSV downloads linked from the API metadata and creates tidy derived outputs in `data/`.

## Automation

GitHub Actions runs the production workflow to:

- install Python dependencies
- install Quarto
- fetch and process ONS data
- render the Quarto website
- deploy the site to GitHub Pages

The workflow can be triggered manually and is also scheduled monthly.

## Local Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/fetch_ons_data.py
quarto render
```

## Interview Relevance

This project is designed to evidence skills relevant to a GSS Senior Statistical Officer role:

- reproducible analytical pipelines
- use of official statistics and metadata
- transparent methodology and limitations
- accessible publication
- automation and version control
