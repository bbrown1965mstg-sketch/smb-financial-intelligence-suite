"""Shared pytest fixtures for the Doc-to-Warehouse acceptance tests (Agent 2).

Owns (per CONTRACT.md section 1): ``conftest.py`` alongside
``generate_sample_pdfs.py``, ``test_extractor.py``, ``sample_pdfs/`` and
``ground_truth/``. This file NEVER touches Agent 1's sources; it only
imports their public API (pdf_parser, categorizer, export_warehouse_csv)
per the CONTRACT.md section 6 signatures.

Provides:
  * sys.path bootstrap so the project modules import regardless of where
    pytest is invoked from;
  * a session fixture that (re)generates the sample corpus only when files
    are missing (never clobbers a good corpus on every test run);
  * session fixtures for the ground truth, the extracted transactions
    (all six PDFs processed once, in ground-truth document order), the
    categorized transactions, and the exported warehouse CSVs.
"""

import csv
import importlib
import json
import os
import sys

import pytest

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

SAMPLE_PDF_NAMES = (
    "bank_statement_pacific_trust.pdf",
    "invoice_abc_lumber.pdf",
    "invoice_sierra_supply.pdf",
    "invoice_hilti_equipment.pdf",
    "payroll_week_2026-03-02.pdf",
    "invoice_abc_lumber_resend.pdf",
)


@pytest.fixture(scope="session")
def project_dir() -> str:
    """Absolute path of the project directory (this file's directory)."""
    return PROJECT_DIR


@pytest.fixture(scope="session")
def sample_dir(project_dir) -> str:
    """Directory holding the generated sample PDFs."""
    return os.path.join(project_dir, "sample_pdfs")


@pytest.fixture(scope="session")
def ensure_samples(sample_dir, project_dir) -> dict:
    """Generate the sample corpus only if it is missing or incomplete.

    Returns a dict with ``sample_dir`` and ``ground_truth_path``.
    """
    ground_truth_path = os.path.join(project_dir, "ground_truth", "ground_truth.json")
    missing = [name for name in SAMPLE_PDF_NAMES
               if not os.path.isfile(os.path.join(sample_dir, name))]
    if missing or not os.path.isfile(ground_truth_path):
        generator = importlib.import_module("generate_sample_pdfs")
        result = generator.generate_all()
        written = {os.path.basename(path) for path in result["files"]}
        assert written == set(SAMPLE_PDF_NAMES), (
            "generate_all() produced %r, expected %r" % (sorted(written), sorted(SAMPLE_PDF_NAMES)))
    return {"sample_dir": sample_dir, "ground_truth_path": ground_truth_path}


@pytest.fixture(scope="session")
def ground_truth(ensure_samples) -> dict:
    """The parsed ground_truth/ground_truth.json written by the generator."""
    with open(ensure_samples["ground_truth_path"], "r", encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="session")
def extracted_transactions(ensure_samples, ground_truth) -> list:
    """All transactions extracted from the six sample PDFs, in ground-truth
    document order (so the invoice re-send is always a later occurrence for
    duplicate detection, matching the pipeline's document ordering)."""
    pdf_parser = importlib.import_module("pdf_parser")
    transactions: list = []
    for doc in ground_truth["documents"]:
        pdf_path = os.path.join(ensure_samples["sample_dir"], doc["file"])
        transactions.extend(pdf_parser.extract_transactions(pdf_path))
    return transactions


@pytest.fixture(scope="session")
def categorized_transactions(extracted_transactions) -> list:
    """Extracted transactions after categorizer.categorize_all (vendor
    normalization + categories + duplicate detection)."""
    categorizer = importlib.import_module("categorizer")
    return categorizer.categorize_all(extracted_transactions)


@pytest.fixture(scope="session")
def exported(ensure_samples, categorized_transactions, tmp_path_factory) -> dict:
    """Warehouse CSVs written once per session into a throwaway temp dir.

    Uses export_warehouse_csv.export_all on the categorized transactions
    (the natural pipeline order: extract -> categorize -> export). Output
    never goes into sample_pdfs/ or ground_truth/.
    """
    export_warehouse_csv = importlib.import_module("export_warehouse_csv")
    out_dir = str(tmp_path_factory.mktemp("warehouse_export"))
    paths = export_warehouse_csv.export_all(categorized_transactions, out_dir)

    def _read(path):
        with open(path, "r", newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        with open(path, "r", newline="", encoding="utf-8") as fh:
            header = next(csv.reader(fh))
        return rows, header

    financial_rows, financial_header = _read(paths["financial_ledger"])
    labor_rows, labor_header = _read(paths["labor_logs"])
    return {
        "paths": paths,
        "out_dir": out_dir,
        "financial_rows": financial_rows,
        "financial_header": financial_header,
        "labor_rows": labor_rows,
        "labor_header": labor_header,
    }
