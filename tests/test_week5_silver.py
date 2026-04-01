"""Tests for Week 5 — Silver layer transformations."""

import os
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from tests.notebook_utils import find_cell

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_W5_LAB = os.path.join(_REPO_ROOT, "labs", "week5", "week5_lab.ipynb")


# ===========================================================================
# Helpers
# ===========================================================================

def _run_cell(spark, pattern):
    sql = find_cell(_W5_LAB, pattern)
    assert sql is not None, f"Could not find cell matching: {pattern}"
    spark.sql(sql)


def _run_silver_stores(spark):
    _run_cell(spark, "silver_stores_merge")


def _run_silver_categories(spark):
    _run_cell(spark, "silver_categories_merge")


def _run_silver_books(spark):
    _run_cell(spark, "silver_books_merge")


def _run_silver_customers(spark):
    _run_cell(spark, "silver_customers_merge")


def _run_orders_unified_view(spark):
    _run_cell(spark, "silver_orders_unified_view")


def _run_silver_orders(spark):
    _run_orders_unified_view(spark)
    _run_cell(spark, "silver_orders_merge")


def _run_order_items_exploded_view(spark):
    _run_cell(spark, "silver_order_items_exploded_view")


def _run_silver_order_items(spark):
    _run_order_items_exploded_view(spark)
    _run_cell(spark, "silver_order_items_merge")


# ===========================================================================
# Tests — silver.stores (DO MODIFY - implement these!)
# ===========================================================================

def test_stores_merge(spark):
    _run_silver_stores(spark)
    row = spark.sql("SELECT store_nbr, name FROM silver.stores WHERE store_nbr = 'S001'").collect()
    col_names = spark.sql("SELECT * FROM silver.stores").columns
    # row is a list of Row objects; row[0].name is a string; col_names is a list of strings
    # TODO: assert that exactly one row exists for S001 with name 'Downtown Books',
    # and that 'ingestion_timestamp' and 'source_filename' are NOT in col_names


# ---------------------------------------------------------------------------
# Tests — silver.categories
# ---------------------------------------------------------------------------

def test_categories_merge(spark):
    _run_silver_categories(spark)
    rows = spark.sql("SELECT category_id, parent_category_id FROM silver.categories").collect()
    space_opera = [r for r in rows if r.category_id == "11"][0]
    fiction = [r for r in rows if r.category_id == "1"][0]
    # space_opera and fiction are Row objects; .parent_category_id is a string
    # TODO: assert space_opera.parent_category_id and fiction.parent_category_id are correct


# ---------------------------------------------------------------------------
# Tests — silver.books
# ---------------------------------------------------------------------------

def test_books_filters_invalid_isbn(spark):
    _run_silver_books(spark)
    isbns = {r.isbn for r in spark.sql("SELECT isbn FROM silver.books").collect()}
    # isbns is a Python set of strings
    # TODO: assert valid ISBNs are in `isbns` and 'BADISBN' is not


def test_books_trims_whitespace(spark):
    spark.sql("""
        INSERT INTO bronze.books VALUES
        ('978-0-00-000099-9', '  Padded Title  ', '  Padded Author  ', ' 11 ',
         current_timestamp(), 'books.csv')
    """)
    _run_silver_books(spark)
    row = spark.sql(
        "SELECT title, author, category_id FROM silver.books WHERE isbn = '978-0-00-000099-9'"
    ).collect()
    # row is a list of Row objects; .title, .author, .category_id are strings
    # TODO: assert that row has exactly 1 result and row[0].title, row[0].author, row[0].category_id
    # are trimmed of whitespace


# ---------------------------------------------------------------------------
# Tests — silver.customers
# ---------------------------------------------------------------------------

def test_customers_takes_most_recent(spark):
    _run_silver_customers(spark)
    alice = spark.sql(
        "SELECT email, name, address, city FROM silver.customers WHERE email = 'alice@example.com'"
    ).collect()
    # alice is a list of Row objects; .name, .address, .city are strings
    # TODO: assert alice has exactly 1 row (deduplication worked) and alice[0].name, .address, .city
    # match the MORE RECENT order (ONL-002, 2025-07-15): 'Alice New', '200 New Ave', 'NewCity'


# ---------------------------------------------------------------------------
# Tests — silver.orders (unified from online + instore)
# ---------------------------------------------------------------------------

def test_orders_unified(spark):
    _run_silver_orders(spark)
    order_ids = {r.order_id for r in spark.sql("SELECT order_id FROM silver.orders").collect()}
    channels = {r.order_channel for r in spark.sql("SELECT order_channel FROM silver.orders").collect()}
    # order_ids and channels are Python sets of strings
    # TODO: assert all 4 order IDs (ONL-001, ONL-002, INS-001, INS-002) are in order_ids,
    # and channels contains exactly {'online', 'in-store'}


def test_orders_online_sentinel(spark):
    _run_silver_orders(spark)
    store_nbrs = [r.store_nbr for r in spark.sql(
        "SELECT store_nbr FROM silver.orders WHERE order_channel = 'online'"
    ).collect()]
    # store_nbrs is a list of strings
    # TODO: assert that all store_nbrs for online orders equal 'online'


def test_orders_instore_null_email_sentinel(spark):
    _run_silver_orders(spark)
    customer_email = spark.sql(
        "SELECT customer_email FROM silver.orders WHERE order_id = 'INS-001'"
    ).collect()[0].customer_email
    customer_email_2 = spark.sql(
        "SELECT customer_email FROM silver.orders WHERE order_id = 'INS-002'"
    ).collect()[0].customer_email
    # customer_email and customer_email_2 are strings
    # TODO: assert that customer_email (INS-001, which had NULL) equals 'in-store',
    # and customer_email_2 (INS-002, which had a real email) equals 'bob@example.com'


# ---------------------------------------------------------------------------
# Tests — silver.order_items (exploded from JSON)
# ---------------------------------------------------------------------------

def test_order_items_exploded(spark):
    _run_silver_order_items(spark)
    order_ids = {r.order_id for r in spark.sql("SELECT order_id FROM silver.order_items").collect()}
    ins_002_count = spark.sql(
        "SELECT COUNT(*) AS cnt FROM silver.order_items WHERE order_id = 'INS-002'"
    ).collect()[0].cnt
    # order_ids is a Python set of strings; ins_002_count is an integer
    # TODO: assert ONL-001 and INS-001 are in order_ids, and ins_002_count equals 2
    # (INS-002 had 2 items in its JSON array, so it should explode into 2 rows)


def test_order_items_drops_title(spark):
    _run_silver_order_items(spark)
    cols = spark.sql("SELECT * FROM silver.order_items").columns
    # cols is a list of strings
    # TODO: assert that 'title' is not in cols (title is redundant with silver.books and should be dropped)


def test_order_items_decimal_cast(spark):
    _run_silver_order_items(spark)
    schema = spark.sql("SELECT * FROM silver.order_items").schema
    unit_price_field = [f for f in schema.fields if f.name == "unit_price"][0]
    # unit_price_field is a StructField; str(unit_price_field.dataType) is a string
    # TODO: assert that "DecimalType" is in str(unit_price_field.dataType)


# ===========================================================================
# Test fixtures — populate bronze tables with test data
# (COMPLETE - Do not modify)
# ===========================================================================

@pytest.fixture(autouse=True)
def bronze_data(spark):
    """Automatically populate bronze tables for all silver tests.

    This fixture runs before every test in this module, inserting test
    data into all bronze tables that silver transformations read from.
    """


    # --- bronze.categories: 3-level hierarchy ---
    spark.sql("""
        INSERT INTO bronze.categories VALUES
        ('1',  'Fiction',         '',  current_timestamp(), 'categories.csv'),
        ('3',  'Science Fiction', '1', current_timestamp(), 'categories.csv'),
        ('11', 'Space Opera',     '3', current_timestamp(), 'categories.csv')
    """)

    # --- bronze.stores ---
    spark.sql("""
        INSERT INTO bronze.stores VALUES
        ('S001', 'Downtown Books', '100 Main St', 'Springfield', 'IL', '62701',
         current_timestamp(), 'stores.csv')
    """)

    # --- bronze.books: 5 rows, only 2 should pass ISBN+title validation ---
    spark.sql("""
        INSERT INTO bronze.books VALUES
        ('978-0-00-000001-1', 'Test Book One',  'Author A', '11', current_timestamp(), 'books.csv'),
        ('978-0-00-000002-2', 'Test Book Two',  'Author B', '11', current_timestamp(), 'books.csv'),
        ('BADISBN',           'Bad ISBN Book',   'Author C', '11', current_timestamp(), 'books.csv'),
        ('978-0-00-000004-4', '',                'Author D', '11', current_timestamp(), 'books.csv'),
        ('978-0-00-000005-5', '   ',             'Author E', '11', current_timestamp(), 'books.csv')
    """)

    # --- bronze.online_orders: 2 orders from same customer, different timestamps ---
    spark.sql("""
        INSERT INTO bronze.online_orders VALUES
        ('ONL-001', CAST('2025-06-01 10:00:00' AS TIMESTAMP),
         'alice@example.com', 'Alice Old', '100 Old St', 'OldCity', 'IL', '60001',
         '[{"isbn":"978-0-00-000001-1","title":"Test Book One","quantity":2,"unit_price":19.99}]',
         'credit_card', CAST(39.98 AS DECIMAL(10,2)),
         current_timestamp(), 'online_orders_1.csv'),
        ('ONL-002', CAST('2025-07-15 14:00:00' AS TIMESTAMP),
         'alice@example.com', 'Alice New', '200 New Ave', 'NewCity', 'IL', '60002',
         '[{"isbn":"978-0-00-000002-2","title":"Test Book Two","quantity":1,"unit_price":24.99}]',
         'debit_card', CAST(24.99 AS DECIMAL(10,2)),
         current_timestamp(), 'online_orders_2.csv')
    """)

    # --- bronze.instore_orders: 2 orders ---
    # One with NULL email (should become 'in-store' sentinel)
    # One with email
    spark.sql("""
        INSERT INTO bronze.instore_orders VALUES
        ('INS-001', CAST('2025-06-15 11:00:00' AS TIMESTAMP),
         'S001', NULL,
         '[{"isbn":"978-0-00-000001-1","title":"Test Book One","quantity":1,"unit_price":19.99}]',
         'cash', CAST(19.99 AS DECIMAL(10,2)), 'Bob Jones',
         current_timestamp(), 'instore_orders_1.csv'),
        ('INS-002', CAST('2025-06-16 12:00:00' AS TIMESTAMP),
         'S001', 'bob@example.com',
         '[{"isbn":"978-0-00-000001-1","title":"Test Book One","quantity":3,"unit_price":19.99},{"isbn":"978-0-00-000002-2","title":"Test Book Two","quantity":1,"unit_price":24.99}]',
         'credit_card', CAST(84.96 AS DECIMAL(10,2)), 'Jane Doe',
         current_timestamp(), 'instore_orders_2.csv')
    """)
