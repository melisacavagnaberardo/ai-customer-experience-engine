"""
R__sql_utils.py
===============
Unit tests for pure Python utility functions.

Covers:
- ``split_sql``  — SQL statement splitter in ``R__deploy.py``
- ``clean_value`` — NULL normaliser in ``R__deploy.py``
- ``view_name``   — VW_ prefix logic mirrored from ``SP_CREATE_VIEWS``

All tests run without a Snowflake connection.
"""

import math
import sys
from pathlib import Path
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Make root importable and mock heavy deps before importing R__deploy
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

for _mod in ["snowflake", "snowflake.connector", "snowflake.connector.pandas_tools"]:
    sys.modules.setdefault(_mod, MagicMock())

from R__deploy import split_sql, clean_value  # noqa: E402


# ---------------------------------------------------------------------------
# view_name — mirrors the helper embedded in SP_CREATE_VIEWS
# Keeping it here avoids coupling tests to the SQL string; the SP is the
# source of truth and this function is validated against it manually.
# ---------------------------------------------------------------------------
def view_name(raw_name: str) -> str:
    """Return ``VW_<name>``, stripping any existing ``VW_`` prefix.

    Mirrors the ``view_name()`` helper inside
    ``2__infra/migrations/1.10__sp_create_silver_views.sql``.

    Args:
        raw_name: Object name as it appears in ``INFORMATION_SCHEMA``.

    Returns:
        View name with exactly one ``VW_`` prefix.
    """
    stripped = raw_name.upper()
    if stripped.startswith("VW_"):
        stripped = stripped[3:]
    return f"VW_{stripped}"


# ===========================================================================
# split_sql
# ===========================================================================

class TestSplitSql:
    """Tests for ``split_sql`` — statement splitter used by ``run_sql_file``."""

    def test_single_statement(self):
        """A single terminated statement returns one element."""
        sql = "SELECT 1;"
        result = split_sql(sql)
        assert len(result) == 1
        assert result[0] == "SELECT 1"

    def test_multiple_statements(self):
        """Semicolons correctly delimit multiple statements."""
        sql = "CREATE TABLE A (id INT);\nCREATE TABLE B (id INT);"
        result = split_sql(sql)
        assert len(result) == 2

    def test_ignores_comment_lines(self):
        """Lines starting with ``--`` are skipped entirely."""
        sql = "-- this is a comment\nSELECT 1;"
        result = split_sql(sql)
        assert len(result) == 1
        assert "comment" not in result[0]

    def test_ignores_blank_lines(self):
        """Blank lines between statements do not produce empty entries."""
        sql = "\n\nSELECT 1;\n\nSELECT 2;\n\n"
        result = split_sql(sql)
        assert len(result) == 2

    def test_unterminated_last_statement(self):
        """A statement without a trailing semicolon is still captured."""
        sql = "SELECT 1;\nSELECT 2"
        result = split_sql(sql)
        assert len(result) == 2

    def test_empty_string_returns_empty_list(self):
        """Empty input produces no statements."""
        assert split_sql("") == []

    def test_only_comments_returns_empty_list(self):
        """Input containing only comments produces no statements."""
        sql = "-- comment one\n-- comment two"
        assert split_sql(sql) == []

    def test_semicolon_stripped_from_result(self):
        """Returned statements must not include the trailing semicolon."""
        result = split_sql("SELECT 42;")
        assert not result[0].endswith(";")

    def test_environment_placeholder_preserved(self):
        """``{{ environment }}`` tokens pass through untouched."""
        sql = "USE DATABASE DB_{{ environment }}_DES;"
        result = split_sql(sql)
        assert "{{ environment }}" in result[0]


# ===========================================================================
# clean_value
# ===========================================================================

class TestCleanValue:
    """Tests for ``clean_value`` — NULL normaliser used before Snowflake inserts."""

    def test_none_stays_none(self):
        """``None`` input returns ``None``."""
        assert clean_value(None) is None

    def test_float_nan_becomes_none(self):
        """Float NaN is treated as missing."""
        assert clean_value(float("nan")) is None

    def test_math_nan_becomes_none(self):
        """``math.nan`` is treated as missing."""
        assert clean_value(math.nan) is None

    def test_string_nan_becomes_none(self):
        """String ``'nan'`` (case-insensitive) becomes ``None``."""
        assert clean_value("nan") is None
        assert clean_value("NaN") is None
        assert clean_value("NAN") is None

    def test_string_na_becomes_none(self):
        """String ``'na'`` becomes ``None``."""
        assert clean_value("na") is None
        assert clean_value("NA") is None

    def test_string_null_becomes_none(self):
        """String ``'null'`` becomes ``None``."""
        assert clean_value("null") is None
        assert clean_value("NULL") is None

    def test_empty_string_becomes_none(self):
        """Empty or whitespace-only strings become ``None``."""
        assert clean_value("") is None
        assert clean_value("   ") is None

    def test_valid_string_unchanged(self):
        """Regular strings are returned as-is."""
        assert clean_value("hello") == "hello"

    def test_zero_unchanged(self):
        """Numeric zero is a valid value, not NULL."""
        assert clean_value(0) == 0

    def test_false_unchanged(self):
        """Boolean ``False`` is a valid value, not NULL."""
        assert clean_value(False) is False

    def test_valid_float_unchanged(self):
        """A normal float is returned unchanged."""
        assert clean_value(3.14) == 3.14


# ===========================================================================
# view_name
# ===========================================================================

class TestViewName:
    """Tests for the ``VW_`` prefix logic mirrored from ``SP_CREATE_VIEWS``."""

    def test_plain_table_gets_prefix(self):
        """A table name without prefix receives ``VW_``."""
        assert view_name("TB_REVIEWS") == "VW_TB_REVIEWS"

    def test_existing_vw_prefix_not_doubled(self):
        """An object already named ``VW_X`` does not become ``VW_VW_X``."""
        assert view_name("VW_TB_REVIEWS") == "VW_TB_REVIEWS"

    def test_lowercase_input_normalised(self):
        """Lowercase input is uppercased before prefixing."""
        assert view_name("tb_products") == "VW_TB_PRODUCTS"

    def test_lowercase_vw_prefix_stripped(self):
        """Lowercase ``vw_`` prefix is also stripped correctly."""
        assert view_name("vw_tb_products") == "VW_TB_PRODUCTS"

    def test_result_always_uppercase(self):
        """Result is always fully uppercase regardless of input casing."""
        result = view_name("mixed_Case_Name")
        assert result == result.upper()

    def test_vw_in_middle_not_stripped(self):
        """``VW_`` only stripped when it is a prefix, not when embedded."""
        assert view_name("TB_VW_SOMETHING") == "VW_TB_VW_SOMETHING"
