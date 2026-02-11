# LinkedIn Job Pipeline (Python)

**Python • Pandas • Data Pipeline • Feature Engineering**

---

## Overview

This project builds a reproducible data pipeline using Python and Pandas to transform raw job posting data into structured analytical outputs.

The pipeline performs:

- data ingestion from raw CSV sources
- feature engineering on timestamp and remote indicators
- grouped aggregations to answer analytical questions
- automated generation of output artifacts

The goal of this project is to demonstrate foundational data engineering workflows, including structured transformations, reproducible execution, and pipeline-style organization.

---

## Dataset

This project uses the LinkedIn Job Scraper dataset:

https://github.com/ArshKA/LinkedIn-Job-Scraper

Due to file size, the full raw dataset is not included in this repository.

### Running with the Full Dataset

1. Download `postings.csv` from the source repository.
2. Place the file inside the `data/` directory:

```
data/postings.csv
```

A smaller sample file is included for quick testing and demonstration.

---

## Pipeline Structure

The pipeline follows a sequential workflow:

1. Data Loading  
2. Column Selection & Cleaning  
3. Feature Engineering  
4. Analytical Aggregations  
5. Output Generation  

All transformations are executed from a single script:

```
job_pipeline.py
```

Running the script from top to bottom recreates the full workflow.

---

## Feature Engineering

Key engineered fields include:

**`is_remote`**  
Derived from the `remote_allowed` indicator to create a boolean remote-work flag.

**`posting_age_days`**  
Computed by converting epoch timestamps into datetime format and measuring recency relative to the newest posting in the dataset.

These transformations simulate common data engineering enrichment steps performed before analysis.

---

## Analytical Outputs

The pipeline generates several summary artifacts:

- Remote share by work type
- Average posting age by remote status
- Cleaned dataset sample for downstream use

These outputs are written automatically to the `output/` folder.

---

## Repository Structure

```
job-pipeline-project/
│
├── output/
│   ├── clean_postings_sample.csv
│   ├── remote_share_by_work_type.csv
│   └── posting_age_by_remote.csv
│
├── job_pipeline.py
└── README.md
```

---

## How to Run

Install dependencies:

```
pip install pandas
```

Run the pipeline:

```
python job_pipeline.py
```

Outputs will be generated inside:

```
output/
```

---

## Tools & Skills Demonstrated

### Python / Pandas
- Data transformation workflows
- Datetime processing
- Boolean feature engineering
- Groupby aggregations
- File-based output pipelines

### Data Engineering Concepts
- Reproducible execution
- Structured pipeline design
- Derived feature creation
- Lightweight ETL-style workflow

---

## Design Decisions

- Columns with excessive missingness were excluded to maintain signal quality.
- Raw timestamps were preserved while creating derived datetime features.
- Missing `remote_allowed` values are treated as non-remote.
- A dataset sample is exported to keep repository size manageable.

---

## Portfolio Context

This project represents a transition from analytical workflows toward structured data engineering practices. It focuses on building clear transformation pipelines, automating output generation, and organizing code to support repeatable execution.
