# PII Detection & Data Quality Validation Pipeline

This repository contains a full end-to-end Python data engineering pipeline designed to profile, validate, clean, and mask sensitive raw customer data (`customers_raw.csv`).

This was built as a solution for a Data Engineering project simulating the intake of messy, real-world banking/fintech customer records.

## Project Structure

* `customers_raw.csv` - The original, uncleaned input file.
* `src/` - The core Python logic modules.
  * `profiler.py` - Reads the raw input and outputs an exploratory data quality profile report detailing missing data % and regex format failures.
  * `detector.py` - Explicitly detects and enumerates the volume of Personally Identifiable Information (PII) elements exposed in the dataset.
  * `validator.py` - Utilizes `pandera` to enforce strict DataFrame schema constraints. It outputs detailed logging indicating exactly which row and column failed validation and why.
  * `cleaner.py` - Houses the core logic to sanitize date structures (forcing `YYYY-MM-DD`), phone numbers (forcing `XXX-XXX-XXXX`), and handles graceful string casing. It also implements fallback procedures for missing null elements and populates `needs_review` flags.
  * `masker.py` - Translates cleaned strings into GDPR-compliant obfuscated strings (e.g. converting `jane.smith@email.com` to `j***@email.com`).
  * `pipeline.py` - The master orchestrator script wrapping all steps sequentially.
  * `test_pipeline.py` - The automated testing suite built with python's foundational `unittest` module.
* `output/` - The directory where the finalized `customers_cleaned.csv` and `customers_masked.csv` are deposited.
* `reports/` - The directory logging the comprehensive documentation outputs of every step of the pipeline.
* `reflection.md` - A governance document evaluating the trade-offs implemented during the data cleaning and PII masking sequences.

## Setup Requirements

You must install the project dependencies (specifically `pandas` and `pandera`) to execute this pipeline.

```bash
pip install -r requirements.txt
```

## Running the Pipeline

Simply run the master orchestrator script. It will automatically traverse `.csv` generation and produce all the finalized reports inside the `/reports` directory.

```bash
python src/pipeline.py
```

## Running the Tests

To test the specific string manipulation logics (without invoking full file generations) execute the test file explicitly:

```bash
python -m unittest src/test_pipeline.py
```
