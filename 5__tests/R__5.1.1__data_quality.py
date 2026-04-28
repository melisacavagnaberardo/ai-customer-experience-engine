"""
R__data_quality.py
==================
Data quality tests for the CSV seed files in ``7__data/seeds/``.

Validates that the source data loaded into Snowflake meets the minimum
structural and business-rule requirements before any transformation runs.

Covers:
- ``REVIEWS.csv``  — column presence, row count, STARS range, non-null IDs
- ``PRODUCTS.csv`` — column presence, ASIN uniqueness, no fully-empty rows

All tests run without a Snowflake connection — they operate on local files.
"""

import pytest
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT       = Path(__file__).resolve().parents[1]
SEEDS_DIR  = ROOT / "7__data" / "seeds"

REVIEWS_FILE  = SEEDS_DIR / "REVIEWS.csv"
PRODUCTS_FILE = SEEDS_DIR / "PRODUCTS.csv"

# Columns that the SQL models reference explicitly
REVIEWS_REQUIRED_COLS  = {"ID", "ASIN", "STARS", "BODY", "VERIFIED_PURCHASE", "FOUND_HELPFUL"}
PRODUCTS_REQUIRED_COLS = {"ASIN"}

EXPECTED_SAMPLE_SIZE      = 5000   # target — see context.md for cleaning decisions
EXPECTED_SAMPLE_TOLERANCE = 10     # allow minor variance from deduplication


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def reviews() -> pd.DataFrame:
    """Load REVIEWS.csv once for all tests in this module.

    Returns:
        DataFrame with uppercase column names matching Snowflake conventions.
    """
    df = pd.read_csv(REVIEWS_FILE, on_bad_lines="skip")
    df.columns = [c.upper() for c in df.columns]
    return df


@pytest.fixture(scope="module")
def products() -> pd.DataFrame:
    """Load PRODUCTS.csv once for all tests in this module.

    Returns:
        DataFrame with uppercase column names matching Snowflake conventions.
    """
    df = pd.read_csv(PRODUCTS_FILE, on_bad_lines="skip")
    df.columns = [c.upper() for c in df.columns]
    return df


# ===========================================================================
# REVIEWS — file existence
# ===========================================================================

class TestReviewsFileExists:
    """Verify that the REVIEWS seed file is present before running any test."""

    def test_file_exists(self):
        """REVIEWS.csv must exist in the seeds directory."""
        assert REVIEWS_FILE.exists(), f"Missing seed file: {REVIEWS_FILE}"

    def test_file_not_empty(self):
        """REVIEWS.csv must not be an empty file."""
        assert REVIEWS_FILE.stat().st_size > 0


# ===========================================================================
# REVIEWS — schema
# ===========================================================================

class TestReviewsSchema:
    """Validate that REVIEWS.csv contains all columns referenced by the SQL models."""

    def test_required_columns_present(self, reviews):
        """All columns referenced in V3.3.3 / V3.3.4 must be present."""
        missing = REVIEWS_REQUIRED_COLS - set(reviews.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_no_duplicate_column_names(self, reviews):
        """Column names must be unique (duplicate headers corrupt inserts)."""
        assert len(reviews.columns) == len(set(reviews.columns))


# ===========================================================================
# REVIEWS — row count
# ===========================================================================

class TestReviewsRowCount:
    """Validate the number of rows after stratified sampling."""

    def test_has_rows(self, reviews):
        """Dataset must contain at least one row."""
        assert len(reviews) > 0

    def test_sample_size(self, reviews):
        """Dataset must be within tolerance of EXPECTED_SAMPLE_SIZE rows.

        A small variance is allowed when the source has duplicate IDs that are
        removed before sampling. See ``context.md`` for details.
        """
        lo = EXPECTED_SAMPLE_SIZE - EXPECTED_SAMPLE_TOLERANCE
        hi = EXPECTED_SAMPLE_SIZE
        assert lo <= len(reviews) <= hi, (
            f"Expected {lo}-{hi} rows, got {len(reviews)}. "
            "See context.md — cleaning decisions section."
        )


# ===========================================================================
# REVIEWS — business rules
# ===========================================================================

class TestReviewsBusinessRules:
    """Validate domain constraints that the Snowflake models assume."""

    def test_id_not_null(self, reviews):
        """Every review must have a non-null ID (used as primary key)."""
        assert reviews["ID"].notna().all(), "Found NULL values in ID column"

    def test_asin_not_null(self, reviews):
        """Every review must be linked to a product (ASIN not null)."""
        assert reviews["ASIN"].notna().all(), "Found NULL values in ASIN column"

    def test_stars_range(self, reviews):
        """STARS must be an integer between 1 and 5 inclusive."""
        valid_stars = {1, 2, 3, 4, 5}
        actual_stars = set(reviews["STARS"].dropna().astype(int).unique())
        invalid = actual_stars - valid_stars
        assert not invalid, f"Invalid STARS values found: {invalid}"

    def test_stars_all_ratings_represented(self, reviews):
        """All five star ratings must appear after stratified sampling."""
        present = set(reviews["STARS"].dropna().astype(int).unique())
        missing = {1, 2, 3, 4, 5} - present
        assert not missing, f"Missing star ratings after sampling: {missing}"

    def test_id_unique(self, reviews):
        """Review IDs must be unique — duplicates would be silently skipped by MERGE."""
        duplicates = reviews["ID"].duplicated().sum()
        assert duplicates == 0, f"Found {duplicates} duplicate IDs"

    def test_body_not_all_null(self, reviews):
        """At least 80 % of reviews must have a non-empty BODY for Cortex enrichment."""
        non_empty = reviews["BODY"].notna() & (reviews["BODY"].str.strip() != "")
        ratio = non_empty.sum() / len(reviews)
        assert ratio >= 0.80, f"Too many empty BODY values: only {ratio:.1%} are non-empty"


# ===========================================================================
# PRODUCTS — file existence
# ===========================================================================

class TestProductsFileExists:
    """Verify that the PRODUCTS seed file is present before running any test."""

    def test_file_exists(self):
        """PRODUCTS.csv must exist in the seeds directory."""
        assert PRODUCTS_FILE.exists(), f"Missing seed file: {PRODUCTS_FILE}"

    def test_file_not_empty(self):
        """PRODUCTS.csv must not be an empty file."""
        assert PRODUCTS_FILE.stat().st_size > 0


# ===========================================================================
# PRODUCTS — schema
# ===========================================================================

class TestProductsSchema:
    """Validate that PRODUCTS.csv contains the columns required by the SQL models."""

    def test_required_columns_present(self, products):
        """All columns referenced in the GOLD models must be present."""
        missing = PRODUCTS_REQUIRED_COLS - set(products.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_no_duplicate_column_names(self, products):
        """Column names must be unique."""
        assert len(products.columns) == len(set(products.columns))


# ===========================================================================
# PRODUCTS — business rules
# ===========================================================================

class TestProductsBusinessRules:
    """Validate domain constraints for the products dimension."""

    def test_has_rows(self, products):
        """Dataset must contain at least one row."""
        assert len(products) > 0

    def test_asin_not_null(self, products):
        """ASIN must never be null — it is the join key with REVIEWS."""
        assert products["ASIN"].notna().all(), "Found NULL values in ASIN column"

    def test_asin_unique(self, products):
        """Each product (ASIN) must appear only once in the dimension table."""
        duplicates = products["ASIN"].duplicated().sum()
        assert duplicates == 0, f"Found {duplicates} duplicate ASINs in PRODUCTS"

    def test_no_fully_empty_rows(self, products):
        """No row should have all columns null (indicates a corrupt CSV line)."""
        fully_empty = products.isna().all(axis=1).sum()
        assert fully_empty == 0, f"Found {fully_empty} fully-empty rows"
