"""Workload definitions for pg_stat_ch benchmark."""

import random
from dataclasses import dataclass
from typing import Any

import psycopg


@dataclass
class WorkloadConfig:
    """Configuration for a workload type."""

    name: str
    weight: int  # percentage of total connections
    queries: list[str]


def _random_user_id() -> int:
    return random.randint(1, 10000)


def _random_product_id() -> int:
    return random.randint(1, 1000)


class SelectWorkload:
    """Read-heavy workload: lookups, aggregations, joins."""

    @staticmethod
    def user_lookup(cur: psycopg.Cursor) -> None:
        """Simple user lookup by ID."""
        cur.execute(
            """
            SELECT id, username, email, balance, status
            FROM bench.users WHERE id = %s
            """,
            (_random_user_id(),),
        )
        cur.fetchall()

    @staticmethod
    def status_aggregation(cur: psycopg.Cursor) -> None:
        """Aggregation by status."""
        cur.execute(
            """
            SELECT status, COUNT(*), AVG(balance)::DECIMAL(15,2) as avg_balance
            FROM bench.users
            GROUP BY status
            """
        )
        cur.fetchall()

    @staticmethod
    def user_orders_join(cur: psycopg.Cursor) -> None:
        """Join: user orders with product info."""
        cur.execute(
            """
            SELECT o.id, o.quantity, o.total_price, o.status, i.name, i.price
            FROM bench.orders o
            JOIN bench.inventory i ON o.product_id = i.id
            WHERE o.user_id = %s
            ORDER BY o.created_at DESC
            LIMIT 10
            """,
            (_random_user_id(),),
        )
        cur.fetchall()

    @staticmethod
    def transaction_range(cur: psycopg.Cursor) -> None:
        """Range query on transactions with aggregation."""
        days = random.randint(1, 30)
        cur.execute(
            """
            SELECT type, COUNT(*), SUM(amount)::DECIMAL(15,2) as total
            FROM bench.transactions
            WHERE created_at > NOW() - INTERVAL '%s days'
            GROUP BY type
            """,
            (days,),
        )
        cur.fetchall()

    @staticmethod
    def inventory_search(cur: psycopg.Cursor) -> None:
        """Pattern search on inventory."""
        pattern = str(random.randint(1, 100))
        cur.execute(
            """
            SELECT id, name, category, price
            FROM bench.inventory
            WHERE name LIKE %s
            LIMIT 20
            """,
            (f"%{pattern}%",),
        )
        cur.fetchall()

    @classmethod
    def all_queries(cls) -> list:
        """Return all SELECT query methods."""
        return [
            cls.user_lookup,
            cls.status_aggregation,
            cls.user_orders_join,
            cls.transaction_range,
            cls.inventory_search,
        ]


class InsertWorkload:
    """Write workload: inserts."""

    @staticmethod
    def insert_order(cur: psycopg.Cursor) -> None:
        """Insert a new order."""
        cur.execute(
            """
            INSERT INTO bench.orders (user_id, product_id, quantity, total_price, status)
            VALUES (%s, %s, %s, %s, 'pending')
            """,
            (
                _random_user_id(),
                _random_product_id(),
                random.randint(1, 5),
                random.randint(10, 500),
            ),
        )

    @staticmethod
    def insert_transaction(cur: psycopg.Cursor) -> None:
        """Insert a new transaction."""
        types = ["credit", "debit", "refund"]
        cur.execute(
            """
            INSERT INTO bench.transactions (user_id, amount, type, description)
            VALUES (%s, %s, %s, %s)
            """,
            (
                _random_user_id(),
                random.randint(1, 1000),
                random.choice(types),
                f"Benchmark transaction",
            ),
        )

    @classmethod
    def all_queries(cls) -> list:
        """Return all INSERT query methods."""
        return [cls.insert_order, cls.insert_transaction]


class UpdateWorkload:
    """Update workload: modifying existing records."""

    @staticmethod
    def update_balance(cur: psycopg.Cursor) -> None:
        """Update user balance."""
        cur.execute(
            """
            UPDATE bench.users
            SET balance = balance + %s, updated_at = NOW()
            WHERE id = %s
            """,
            (random.randint(-100, 100), _random_user_id()),
        )

    @staticmethod
    def update_order_status(cur: psycopg.Cursor) -> None:
        """Update order status."""
        statuses = ["processing", "shipped", "completed", "cancelled"]
        cur.execute(
            """
            UPDATE bench.orders
            SET status = %s
            WHERE id = %s
            """,
            (random.choice(statuses), random.randint(1, 50000)),
        )

    @staticmethod
    def update_inventory(cur: psycopg.Cursor) -> None:
        """Update inventory quantity."""
        cur.execute(
            """
            UPDATE bench.inventory
            SET quantity = GREATEST(0, quantity + %s)
            WHERE id = %s
            """,
            (random.randint(-10, 10), _random_product_id()),
        )

    @classmethod
    def all_queries(cls) -> list:
        """Return all UPDATE query methods."""
        return [cls.update_balance, cls.update_order_status, cls.update_inventory]


class DeleteWorkload:
    """Delete workload: cleanup operations."""

    @staticmethod
    def delete_old_order(cur: psycopg.Cursor) -> None:
        """Delete one old pending order."""
        days = random.randint(30, 90)
        cur.execute(
            """
            DELETE FROM bench.orders
            WHERE status = 'pending'
              AND created_at < NOW() - INTERVAL '%s days'
              AND id IN (SELECT id FROM bench.orders WHERE status = 'pending' LIMIT 1)
            """,
            (days,),
        )

    @staticmethod
    def delete_old_transaction(cur: psycopg.Cursor) -> None:
        """Delete one old transaction."""
        days = random.randint(60, 180)
        cur.execute(
            """
            DELETE FROM bench.transactions
            WHERE created_at < NOW() - INTERVAL '%s days'
              AND id IN (SELECT id FROM bench.transactions ORDER BY created_at LIMIT 1)
            """,
            (days,),
        )

    @classmethod
    def all_queries(cls) -> list:
        """Return all DELETE query methods."""
        return [cls.delete_old_order, cls.delete_old_transaction]


# Default workload configuration: 40% SELECT, 25% INSERT, 25% UPDATE, 10% DELETE
DEFAULT_WORKLOADS = {
    "select": WorkloadConfig("SELECT", 40, SelectWorkload.all_queries()),
    "insert": WorkloadConfig("INSERT", 25, InsertWorkload.all_queries()),
    "update": WorkloadConfig("UPDATE", 25, UpdateWorkload.all_queries()),
    "delete": WorkloadConfig("DELETE", 10, DeleteWorkload.all_queries()),
}
