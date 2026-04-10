"""Tests for Week 3 — INSERT statement testing.

Each test runs an INSERT from the lab notebook, then queries
filtered_employees to verify the correct rows were inserted.

Uses Employee/HR domain data (different from bookstore in weeks 4-6).
"""

import os
import pytest
from tests.notebook_utils import find_cell

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_W3_LAB = os.path.join(_REPO_ROOT, "labs", "week3", "week3_lab.ipynb")


# ===========================================================================
# Helper to run a SQL cell from the notebook
# ===========================================================================

def _run_cell(spark, pattern):
    sql = find_cell(_W3_LAB, pattern)
    assert sql is not None, f"Could not find cell matching: {pattern}"
    return spark.sql(sql)


# ===========================================================================
# Tests — Basic SQL query validation (DO MODIFY - implement these!)
# ===========================================================================

def test_valid_email_filter(spark):
    """Verify that only employees with valid emails are inserted."""
    _run_cell(spark, "valid_email_filter")
    rows = spark.sql("SELECT * FROM week3_testing.filtered_employees").collect()
    # TODO: assert len(rows) equals the number of employees with a valid email


def test_employees_in_salary_range(spark):
    """Verify that only employees in the salary range are inserted."""
    _run_cell(spark, "employees_in_salary_range")
    rows = spark.sql("SELECT * FROM week3_testing.filtered_employees").collect()
    # TODO: assert len(rows) equals the number of employees with salary between $50,000 and $100,000


def test_recent_hires(spark):
    """Verify that only recent hires are inserted."""
    _run_cell(spark, "recent_hires")
    rows = spark.sql("SELECT * FROM week3_testing.filtered_employees").collect()
    # TODO: assert len(rows) equals 1 and rows[0].employee_id equals 'EMP-006'


def test_engineering_department_filter(spark):
    """Verify that only Engineering employees are inserted."""
    _run_cell(spark, "engineering_department_filter")
    rows = spark.sql("SELECT * FROM week3_testing.filtered_employees").collect()
    # TODO: assert len(rows) equals the number of Engineering employees


# ===========================================================================
# Test fixtures — automatically create schema and populate test data
# (COMPLETE - Do not modify)
# ===========================================================================

@pytest.fixture(autouse=True)
def week3_test_data(spark):
    """Automatically create week3_testing schema and tables for all tests.

    This fixture runs before every test in this module, creating the
    schema, tables, and test data that SQL queries will read from.
    """
    spark.sql("CREATE SCHEMA IF NOT EXISTS week3_testing")

    spark.sql("""
        CREATE OR REPLACE TABLE week3_testing.employees (
            employee_id STRING,
            name STRING,
            email STRING,
            department STRING,
            salary DECIMAL(10,2),
            hire_date DATE
        ) USING DELTA
    """)

    spark.sql("""
        INSERT INTO week3_testing.employees VALUES
        ('EMP-001', 'John Smith', 'john.smith@company.com', 'Engineering', 85000.00, DATE('2025-01-15')),
        ('EMP-002', 'Jane Doe', 'jane.doe@company.com', 'Sales', 65000.00, DATE('2024-03-20')),
        ('EMP-003', 'Bob Wilson', 'invalid-email', 'Marketing', 55000.00, DATE('2023-06-10')),
        ('EMP-004', 'Alice Johnson', 'alice.johnson@company.com', 'Engineering', 120000.00, DATE('2025-08-01')),
        ('EMP-005', 'Charlie Brown', '', 'HR', 45000.00, DATE('2020-05-12')),
        ('EMP-006', 'Diana Prince', NULL, 'Sales', 75000.00, DATE('2026-03-15'))
    """)

    spark.sql("""
        CREATE OR REPLACE TABLE week3_testing.filtered_employees (
            employee_id STRING,
            name STRING,
            email STRING,
            department STRING,
            salary DECIMAL(10,2),
            hire_date DATE
        ) USING DELTA
    """)
