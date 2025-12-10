import csv
import os
import sqlite3
from typing import Callable, Dict, Iterable, List, Sequence, Tuple


DB_FILE = "ecom.db"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def ensure_tables(conn: sqlite3.Connection) -> None:
    """Create tables if they don't already exist."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            name TEXT,
            email TEXT,
            city TEXT
        );

        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            name TEXT,
            category TEXT,
            price REAL
        );

        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            user_id TEXT,
            product_id TEXT,
            quantity INTEGER,
            order_date TEXT
        );

        CREATE TABLE IF NOT EXISTS reviews (
            review_id TEXT PRIMARY KEY,
            user_id TEXT,
            product_id TEXT,
            rating INTEGER,
            comment TEXT
        );

        CREATE TABLE IF NOT EXISTS inventory (
            product_id TEXT PRIMARY KEY,
            stock_quantity INTEGER,
            warehouse TEXT
        );
        """
    )


def load_csv(
    conn: sqlite3.Connection,
    csv_path: str,
    table: str,
    columns: Sequence[str],
    converters: Dict[str, Callable[[str], object]] | None = None,
) -> None:
    """Read a CSV and insert its rows into the given table."""
    full_path = os.path.join(BASE_DIR, csv_path)
    converters = converters or {}

    with open(full_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        rows: List[Tuple[object, ...]] = []
        for row in reader:
            values: List[object] = []
            for col in columns:
                value = row[col]
                if col in converters:
                    value = converters[col](value)
                values.append(value)
            rows.append(tuple(values))

    placeholders = ",".join("?" for _ in columns)
    column_list = ",".join(columns)

    # Clear existing rows to avoid duplication on re-run.
    conn.execute(f"DELETE FROM {table}")
    if rows:
        conn.executemany(
            f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})", rows
        )


def main() -> None:
    conn = sqlite3.connect(os.path.join(BASE_DIR, DB_FILE))
    try:
        ensure_tables(conn)

        load_csv(
            conn,
            "users.csv",
            "users",
            ["user_id", "name", "email", "city"],
        )
        load_csv(
            conn,
            "products.csv",
            "products",
            ["product_id", "name", "category", "price"],
            converters={"price": float},
        )
        load_csv(
            conn,
            "orders.csv",
            "orders",
            ["order_id", "user_id", "product_id", "quantity", "order_date"],
            converters={"quantity": int},
        )
        load_csv(
            conn,
            "reviews.csv",
            "reviews",
            ["review_id", "user_id", "product_id", "rating", "comment"],
            converters={"rating": int},
        )
        load_csv(
            conn,
            "inventory.csv",
            "inventory",
            ["product_id", "stock_quantity", "warehouse"],
            converters={"stock_quantity": int},
        )

        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

