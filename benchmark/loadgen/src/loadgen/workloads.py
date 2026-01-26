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
    return random.randint(1, 100000)


def _random_product_id() -> int:
    return random.randint(1, 10000)


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
            (random.choice(statuses), random.randint(1, 100000)),
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


class HeavyWorkload:
    """Heavy workload: queries that exercise JIT, parallel, temp files, buffer reads."""

    @staticmethod
    def full_table_scan(cur: psycopg.Cursor) -> None:
        """Full table scan to generate buffer reads."""
        cur.execute(
            """
            SELECT COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance),
                   STDDEV(balance), VARIANCE(balance)
            FROM bench.users
            """
        )
        cur.fetchall()

    @staticmethod
    def complex_aggregation(cur: psycopg.Cursor) -> None:
        """Complex aggregation to trigger JIT compilation."""
        cur.execute(
            """
            SELECT
                u.status,
                COUNT(DISTINCT u.id) as users,
                COUNT(o.id) as orders,
                SUM(o.total_price) as revenue,
                AVG(o.total_price) as avg_order,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.total_price) as median_order,
                SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END) as cancelled
            FROM bench.users u
            LEFT JOIN bench.orders o ON u.id = o.user_id
            GROUP BY u.status
            """
        )
        cur.fetchall()

    @staticmethod
    def large_sort(cur: psycopg.Cursor) -> None:
        """Large sort to spill to temp files (work_mem is low)."""
        cur.execute(
            """
            SELECT u.id, u.username, u.balance,
                   COUNT(t.id) as tx_count,
                   SUM(t.amount) as total_amount
            FROM bench.users u
            LEFT JOIN bench.transactions t ON u.id = t.user_id
            GROUP BY u.id, u.username, u.balance
            ORDER BY total_amount DESC NULLS LAST
            LIMIT 100
            """
        )
        cur.fetchall()

    @staticmethod
    def hash_join(cur: psycopg.Cursor) -> None:
        """Hash join across multiple tables."""
        cur.execute(
            """
            SELECT i.category, i.name,
                   COUNT(o.id) as order_count,
                   SUM(o.quantity) as total_qty,
                   SUM(o.total_price) as total_revenue
            FROM bench.inventory i
            JOIN bench.orders o ON i.id = o.product_id
            JOIN bench.users u ON o.user_id = u.id
            WHERE u.status = 'active'
            GROUP BY i.category, i.name
            ORDER BY total_revenue DESC
            LIMIT 50
            """
        )
        cur.fetchall()

    @staticmethod
    def window_functions(cur: psycopg.Cursor) -> None:
        """Window functions to add complexity."""
        cur.execute(
            """
            SELECT id, username, balance,
                   ROW_NUMBER() OVER (ORDER BY balance DESC) as rank,
                   balance - LAG(balance) OVER (ORDER BY balance DESC) as diff_from_prev,
                   SUM(balance) OVER (ORDER BY balance DESC) as running_total,
                   AVG(balance) OVER () as overall_avg
            FROM bench.users
            WHERE status = 'active'
            LIMIT 100
            """
        )
        cur.fetchall()

    @staticmethod
    def subquery_heavy(cur: psycopg.Cursor) -> None:
        """Subquery-heavy query for complexity."""
        cur.execute(
            """
            SELECT u.id, u.username,
                   (SELECT COUNT(*) FROM bench.orders WHERE user_id = u.id) as order_count,
                   (SELECT SUM(amount) FROM bench.transactions WHERE user_id = u.id) as tx_total,
                   (SELECT MAX(created_at) FROM bench.orders WHERE user_id = u.id) as last_order
            FROM bench.users u
            WHERE u.id IN (
                SELECT DISTINCT user_id FROM bench.orders
                WHERE created_at > NOW() - INTERVAL '30 days'
            )
            LIMIT 50
            """
        )
        cur.fetchall()

    @classmethod
    def all_queries(cls) -> list:
        """Return all heavy query methods."""
        return [
            cls.full_table_scan,
            cls.complex_aggregation,
            cls.large_sort,
            cls.hash_join,
            cls.window_functions,
            cls.subquery_heavy,
        ]


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


# Default workload configuration: 30% SELECT, 20% INSERT, 20% UPDATE, 10% DELETE, 20% HEAVY
DEFAULT_WORKLOADS = {
    "select": WorkloadConfig("SELECT", 30, SelectWorkload.all_queries()),
    "insert": WorkloadConfig("INSERT", 20, InsertWorkload.all_queries()),
    "update": WorkloadConfig("UPDATE", 20, UpdateWorkload.all_queries()),
    "delete": WorkloadConfig("DELETE", 10, DeleteWorkload.all_queries()),
    "heavy": WorkloadConfig("HEAVY", 20, HeavyWorkload.all_queries()),
}
