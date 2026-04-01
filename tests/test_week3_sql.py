"""Tests for Week 3 — Basic SQL query testing.

This test file demonstrates how to write tests for SQL queries
in Jupyter notebooks using the tagging framework.

Uses Employee/HR domain data (different from bookstore in weeks 4-6).
"""

import os
from decimal import Decimal

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
    """Verify that email filter only returns valid email formats."""
    rows = _run_cell(spark, "valid_email_filter").collect()
    emails = [r.email for r in rows]
    # rows is a list of Row objects; emails is a list of strings
    # TODO: assert the correct number of employees are returned and every email contains '@'


def test_count_employees(spark):
    """Verify that employee count query returns correct count."""
    row = _run_cell(spark, "count_employees").collect()[0]
    # row is a Row object; row.total_employees is an integer
    # TODO: assert row.total_employees equals the expected total number of employees


def test_employees_in_salary_range(spark):
    """Verify that salary range filter returns correct employees."""
    rows = _run_cell(spark, "employees_in_salary_range").collect()
    salaries = [row.salary for row in rows]
    # rows is a list of Row objects; salaries is a list of Decimal values
    # TODO: assert the correct number of employees are returned, all salaries are between 50000 and 100000,
    # and results are in ascending salary order


def test_recent_hires(spark):
    """Verify that recent hires query filters by date correctly."""
    rows = _run_cell(spark, "recent_hires").collect()
    # rows is a list of Row objects; rows[0].employee_id is a string
    # TODO: assert the correct number of recent hires are returned and check which employee_id appears


def test_average_salary_by_department(spark):
    """Verify that average salary by department is calculated correctly."""
    rows = _run_cell(spark, "average_salary_by_department").collect()
    dept_avgs = {row.department: row.avg_salary for row in rows}
    # dept_avgs is a dict mapping string (department name) to Decimal (average salary)
    # TODO: assert the correct number of departments are returned, check specific avg_salary values
    # (hint: Engineering avg = 102500, Sales avg = 70000), and verify results are ordered descending by avg_salary


def test_employees_with_valid_email(spark):
    """Verify that email filter excludes NULL and empty emails."""
    rows = _run_cell(spark, "employees_with_valid_email").collect()
    # rows is a list of Row objects; rows[0].email is a string or None
    # TODO: assert the correct number of rows are returned and all emails are non-null and non-empty string


# ---------------------------------------------------------------------------
# Additional validation tests
# ---------------------------------------------------------------------------

def test_email_filter_returns_expected_columns(spark):
    """Verify that email filter query returns the expected columns."""
    cols = _run_cell(spark, "valid_email_filter").columns
    # cols is a list of strings
    # TODO: assert that all expected columns are present:
    # employee_id, name, email, department, salary, hire_date


def test_salary_range_filter_ordering(spark):
    """Verify that salary range query results are ordered by salary."""
    rows = _run_cell(spark, "employees_in_salary_range").collect()
    salaries = [row.salary for row in rows]
    # salaries is a list of Decimal values
    # TODO: assert that each salary is <= the next one (ascending order)


def test_recent_hires_sorting(spark):
    """Verify that recent hires are sorted by hire date descending."""
    rows = _run_cell(spark, "recent_hires").collect()
    hire_dates = [row.hire_date for row in rows]
    # hire_dates is a list of Python date objects
    # TODO: if more than one row is returned, assert hire_dates are in descending order


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
    # Create schema
    spark.sql("CREATE SCHEMA IF NOT EXISTS week3_testing")

    # Create and populate employees table
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
        ('EMP-001', 'John Smith', 'john.smith@company.com', 'Engineering', 85000.00, CURRENT_DATE() - INTERVAL 365 DAYS),
        ('EMP-002', 'Jane Doe', 'jane.doe@company.com', 'Sales', 65000.00, CURRENT_DATE() - INTERVAL 730 DAYS),
        ('EMP-003', 'Bob Wilson', 'invalid-email', 'Marketing', 55000.00, CURRENT_DATE() - INTERVAL 1095 DAYS),
        ('EMP-004', 'Alice Johnson', 'alice.johnson@company.com', 'Engineering', 120000.00, CURRENT_DATE() - INTERVAL 180 DAYS),
        ('EMP-005', 'Charlie Brown', '', 'HR', 45000.00, CURRENT_DATE() - INTERVAL 1825 DAYS),
        ('EMP-006', 'Diana Prince', NULL, 'Sales', 75000.00, CURRENT_DATE() - INTERVAL 10 DAYS)
    """)

    # Create and populate departments table
    spark.sql("""
        CREATE OR REPLACE TABLE week3_testing.departments (
            dept_id STRING,
            dept_name STRING,
            manager_name STRING,
            budget DECIMAL(12,2)
        ) USING DELTA
    """)

    spark.sql("""
        INSERT INTO week3_testing.departments VALUES
        ('DEPT-001', 'Engineering', 'Sarah Connor', 500000.00),
        ('DEPT-002', 'Sales', 'Michael Scott', 300000.00),
        ('DEPT-003', 'Marketing', 'Don Draper', 250000.00),
        ('DEPT-004', 'HR', 'Toby Flenderson', 150000.00)
    """)
