"""
Tests for the `dimension_backfill` materialization.

The models select one *batch* of complete attribute observations at a time
(`observation_batch` var), so late arriving observations, corrections at an
existing start point and contradictory observations can be replayed run by run
against a persistent observation store.
"""

import datetime

import pytest

from dbt.tests.util import (
    relation_from_name,
    run_dbt,
    run_dbt_and_capture,
)

seeds__customer_observations_csv = """customer_id,effective_at,name,segment,observation_batch
1,2024-01-01,alice,gold,1
1,2024-03-01,alice,silver,1
2,2024-01-01,bob,bronze,1
1,2024-02-01,alice,platinum,2
1,2024-03-01,alice,gold,3
2,2024-01-01,bobby,bronze,3
1,2024-05-01,alice,gold,4
1,2024-06-01,alice,gold,5
1,2024-06-01,alice,silver,5
"""

seeds__sales_csv = """sale_id,customer_id,sold_at,amount
1,1,2024-01-01,10.0
2,1,2024-02-15,20.0
3,1,2024-03-01,30.0
4,2,2023-06-01,40.0
5,3,2024-01-01,50.0
6,1,2024-04-01,60.0
"""

seeds__plain_source_csv = """id,label
1,one
2,two
"""

models__customer_dim_sql = """
{{ config(
    materialized='dimension_backfill',
    unique_key='customer_id',
    updated_at='effective_at',
    dimension_attributes=['name', 'segment'],
    fact_tables=[{
        'relation': 'sales',
        'occurred_at': 'sold_at',
        'version_column': 'customer_version_id',
    }],
    post_hook=var('post_hook_sql', ''),
) }}

select customer_id, effective_at, name, segment
from {{ ref('customer_observations') }}
where observation_batch = {{ var('observation_batch', 1) }}
"""

models__plain_model_sql = """
{{ config(materialized='table') }}
select * from {{ ref('plain_source') }}
"""

snapshots__plain_snapshot_sql = """
{% snapshot plain_snapshot %}
    {{ config(unique_key='id', strategy='check', check_cols=['label']) }}
    select * from {{ ref('plain_source') }}
{% endsnapshot %}
"""


class DimensionBackfillTestBase:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "customer_observations.csv": seeds__customer_observations_csv,
            "sales.csv": seeds__sales_csv,
            "plain_source.csv": seeds__plain_source_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "customer_dim.sql": models__customer_dim_sql,
            "plain_model.sql": models__plain_model_sql,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"plain_snapshot.sql": snapshots__plain_snapshot_sql}

    def reset(self, project):
        """Recreate the seed tables (dropping the added fact column) and rebuild
        the dimension from the first observation batch only."""
        run_dbt(["seed", "--full-refresh"])
        self.run_batch(project, 1, full_refresh=True)

    def run_batch(self, project, batch, full_refresh=False, extra_vars=None, extra_args=None):
        args = ["run"]
        if full_refresh:
            args.append("--full-refresh")
        variables = {"observation_batch": batch}
        variables.update(extra_vars or {})
        args += ["--vars", str(variables)]
        args += extra_args or []
        return run_dbt(args)

    def history(self, project):
        relation = relation_from_name(project.adapter, "customer_dim")
        rows = project.run_sql(
            f"select customer_id, name, segment, valid_from, valid_to, dimension_version_id"
            f" from {relation} order by customer_id, valid_from",
            fetch="all",
        )
        return [tuple(row) for row in rows]

    def version_ids(self, project):
        return {
            (customer_id, valid_from): version_id
            for customer_id, _, _, valid_from, _, version_id in self.history(project)
        }

    def store(self, project):
        relation = relation_from_name(project.adapter, "customer_dim__dbt_observations")
        rows = project.run_sql(
            f"select customer_id, effective_at, name, segment from {relation}"
            f" order by customer_id, effective_at",
            fetch="all",
        )
        return [tuple(row) for row in rows]

    def sales(self, project):
        relation = relation_from_name(project.adapter, "sales")
        rows = project.run_sql(
            f"select sale_id, customer_id, sold_at, amount, customer_version_id"
            f" from {relation} order by sale_id",
            fetch="all",
        )
        return [tuple(row) for row in rows]

    def sales_version_ids(self, project):
        # sale_id -> (customer_version_id, amount); amount proves no other
        # fact column changes while re-linking.
        return {
            sale_id: (version_id, amount)
            for sale_id, _, _, amount, version_id in self.sales(project)
        }

    def state(self, project):
        return {
            "history": self.history(project),
            "store": self.store(project),
            "sales": self.sales(project),
        }


@pytest.mark.skip_profile("md", "buenavista")
class TestDimensionBackfill(DimensionBackfillTestBase):
    def test_initial_backfill_builds_half_open_history(self, project):
        self.reset(project)

        history = self.history(project)
        assert [
            (customer_id, name, segment, valid_from, valid_to)
            for customer_id, name, segment, valid_from, valid_to, _ in history
        ] == [
            (1, "alice", "gold", datetime.date(2024, 1, 1), datetime.date(2024, 3, 1)),
            (1, "alice", "silver", datetime.date(2024, 3, 1), None),
            (2, "bob", "bronze", datetime.date(2024, 1, 1), None),
        ]
        # every version is identified by a text id derived from key and start
        assert all(version_id for *_, version_id in history)
        assert len({version_id for *_, version_id in history}) == 3

        # The observation store keeps every accepted observation, including
        # ones that did not open a version of their own.
        assert self.store(project) == [
            (1, datetime.date(2024, 1, 1), "alice", "gold"),
            (1, datetime.date(2024, 3, 1), "alice", "silver"),
            (2, datetime.date(2024, 1, 1), "bob", "bronze"),
        ]

    def test_facts_are_linked_to_the_covering_version(self, project):
        self.reset(project)
        versions = self.version_ids(project)
        sales = self.sales_version_ids(project)

        # exactly on a boundary belongs to the version that starts there
        assert sales[1][0] == versions[(1, datetime.date(2024, 1, 1))]
        assert sales[3][0] == versions[(1, datetime.date(2024, 3, 1))]
        # strictly inside an interval
        assert sales[2][0] == versions[(1, datetime.date(2024, 1, 1))]
        assert sales[6][0] == versions[(1, datetime.date(2024, 3, 1))]
        # a fact before the first version has no covering version
        assert sales[4][0] is None
        # a fact whose business key is not in the dimension at all
        assert sales[5][0] is None
        # re-linking only writes the version column
        assert [amount for _, amount in sales.values()] == [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]
        assert [row[2] for row in self.sales(project)] == [
            datetime.date(2024, 1, 1),
            datetime.date(2024, 2, 15),
            datetime.date(2024, 3, 1),
            datetime.date(2023, 6, 1),
            datetime.date(2024, 1, 1),
            datetime.date(2024, 4, 1),
        ]

    def test_late_observation_splits_the_interval_and_keeps_version_ids(self, project):
        self.reset(project)
        before = self.version_ids(project)

        self.run_batch(project, 2)

        history = self.history(project)
        assert [
            (name, valid_from, valid_to) for _, name, _, valid_from, valid_to, _ in history
        ] == [
            ("alice", datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)),
            ("alice", datetime.date(2024, 2, 1), datetime.date(2024, 3, 1)),
            ("alice", datetime.date(2024, 3, 1), None),
            ("bob", datetime.date(2024, 1, 1), None),
        ]

        after = self.version_ids(project)
        # the start points that did not move keep the identifier they had
        assert after[(1, datetime.date(2024, 1, 1))] == before[(1, datetime.date(2024, 1, 1))]
        assert after[(1, datetime.date(2024, 3, 1))] == before[(1, datetime.date(2024, 3, 1))]
        assert after[(2, datetime.date(2024, 1, 1))] == before[(2, datetime.date(2024, 1, 1))]
        # the newly appearing change point gets a new one
        assert after[(1, datetime.date(2024, 2, 1))] not in before.values()

        # the fact that now falls inside the new version is re-linked to it
        assert self.sales_version_ids(project)[2][0] == after[(1, datetime.date(2024, 2, 1))]

    def test_correction_at_existing_start_upserts_without_duplicating(self, project):
        self.reset(project)
        self.run_batch(project, 2)
        before = self.version_ids(project)

        self.run_batch(project, 3)

        # same start point corrected in place: attributes updated, id kept
        history = self.version_ids(project)
        assert history[(1, datetime.date(2024, 3, 1))] == before[(1, datetime.date(2024, 3, 1))]
        assert history[(2, datetime.date(2024, 1, 1))] == before[(2, datetime.date(2024, 1, 1))]

        assert [
            (customer_id, name, valid_from) for customer_id, name, _, valid_from, _, _ in self.history(project)
        ] == [
            (1, "alice", datetime.date(2024, 1, 1)),
            (1, "alice", datetime.date(2024, 2, 1)),
            (1, "alice", datetime.date(2024, 3, 1)),
            (2, "bobby", datetime.date(2024, 1, 1)),
        ]
        assert self.store(project) == [
            (1, datetime.date(2024, 1, 1), "alice", "gold"),
            (1, datetime.date(2024, 2, 1), "alice", "platinum"),
            (1, datetime.date(2024, 3, 1), "alice", "gold"),
            (2, datetime.date(2024, 1, 1), "bobby", "bronze"),
        ]

    def test_identical_consecutive_observation_is_kept_but_not_versioned(self, project):
        self.reset(project)
        self.run_batch(project, 2)
        self.run_batch(project, 3)
        before = self.history(project)

        self.run_batch(project, 4)

        # (1, 2024-05-01) repeats the attributes that are already in effect, so
        # no new version is opened and the current one keeps its boundaries
        assert self.history(project) == before
        assert (1, datetime.date(2024, 5, 1), "alice", "gold") in self.store(project)
        assert len([row for row in self.store(project) if row[0] == 1]) == 4

    def test_empty_batch_rebuilds_the_same_history_from_the_store(self, project):
        self.reset(project)
        self.run_batch(project, 2)
        before = self.state(project)

        # no observations at all in this batch: nothing is dropped, because the
        # history is rebuilt from the store rather than from the source
        self.run_batch(project, 99)

        assert self.state(project) == before

    def test_contradictory_observations_in_one_batch_fail_and_keep_old_state(self, project):
        self.reset(project)
        before = self.state(project)

        _, log = run_dbt_and_capture(
            ["run", "--vars", "{'observation_batch': 5}"],
            expect_pass=False,
        )

        assert "contradictory observations" in log
        assert "(1) at 2024-06-01 has 2 different attribute observations" in log
        assert self.state(project) == before

    def test_in_transaction_post_hook_failure_rolls_everything_back(self, project):
        self.reset(project)
        before = self.state(project)

        _, log = run_dbt_and_capture(
            [
                "run",
                "--vars",
                "{'observation_batch': 2, 'post_hook_sql': 'select * from definitely_missing_table'}",
            ],
            expect_pass=False,
        )

        assert "definitely_missing_table" in log
        # the observation batch, the rebuilt history and the fact re-links were
        # all written in the model transaction, so all of them are gone again
        assert self.state(project) == before

    def test_snapshots_and_plain_models_are_unaffected(self, project):
        self.reset(project)
        self.run_batch(project, 2)

        plain = relation_from_name(project.adapter, "plain_model")
        assert [tuple(row) for row in project.run_sql(f"select * from {plain} order by id", fetch="all")] == [
            (1, "one"),
            (2, "two"),
        ]

        run_dbt(["snapshot"])
        snapshot = relation_from_name(project.adapter, "plain_snapshot")
        columns = [
            row[0] for row in project.run_sql(f"describe {snapshot}", fetch="all")
        ]
        assert "dbt_valid_from" in columns
        assert "dbt_valid_to" in columns
        assert "dimension_version_id" not in columns
        assert [
            tuple(row)
            for row in project.run_sql(
                f"select id, label, dbt_valid_to is null as is_current"
                f" from {snapshot} order by id",
                fetch="all",
            )
        ] == [(1, "one", True), (2, "two", True)]

        # the dimension model itself is not a dbt snapshot: it has no scd columns
        dimension_columns = [
            row[0]
            for row in project.run_sql(
                f"describe {relation_from_name(project.adapter, 'customer_dim')}", fetch="all"
            )
        ]
        assert "dbt_scd_id" not in dimension_columns
        assert "dbt_valid_from" not in dimension_columns


seeds__product_observations_csv = """region,sku,effective_at,price
EU,A,2024-01-01,10.0
EU,A,2024-03-01,12.0
EU,B,2024-01-01,5.0
US,A,2024-01-01,11.0
"""

seeds__orders_csv = """order_id,region,product_code,ordered_at,quantity
1,EU,A,2024-01-10,1
2,EU,A,2024-03-01,2
3,EU,B,2024-01-01,3
4,US,A,2023-01-05,4
"""

models__product_dim_sql = """
{{ config(
    materialized='dimension_backfill',
    unique_key=['region', 'sku'],
    updated_at='effective_at',
    dimension_attributes=['price'],
    fact_tables=[{
        'relation': 'orders',
        'unique_key': ['region', 'product_code'],
        'occurred_at': 'ordered_at',
    }],
) }}

select region, sku, effective_at, price from {{ ref('product_observations') }}
"""


@pytest.mark.skip_profile("md", "buenavista")
class TestDimensionBackfillCompositeKey:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "product_observations.csv": seeds__product_observations_csv,
            "orders.csv": seeds__orders_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"product_dim.sql": models__product_dim_sql}

    def test_composite_business_key_and_renamed_fact_key(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        dimension = relation_from_name(project.adapter, "product_dim")
        rows = [
            tuple(row)
            for row in project.run_sql(
                f"select region, sku, price, valid_from, valid_to, dimension_version_id"
                f" from {dimension} order by region, sku, valid_from",
                fetch="all",
            )
        ]
        assert [(region, sku, price) for region, sku, price, *_ in rows] == [
            ("EU", "A", 10.0),
            ("EU", "A", 12.0),
            ("EU", "B", 5.0),
            ("US", "A", 11.0),
        ]
        versions = {
            (region, sku, valid_from): version_id
            for region, sku, _, valid_from, _, version_id in rows
        }

        orders = relation_from_name(project.adapter, "orders")
        linked = {
            order_id: version_id
            for order_id, version_id in project.run_sql(
                f"select order_id, dimension_version_id from {orders} order by order_id",
                fetch="all",
            )
        }
        # the fact's business key columns are matched by position with the
        # dimension's, so `product_code` links to `sku`
        assert linked[1] == versions[("EU", "A", datetime.date(2024, 1, 1))]
        assert linked[2] == versions[("EU", "A", datetime.date(2024, 3, 1))]
        assert linked[3] == versions[("EU", "B", datetime.date(2024, 1, 1))]
        assert linked[4] is None


models__contract_dim_sql = """
{{ config(
    materialized='dimension_backfill',
    unique_key='customer_id',
    updated_at='effective_at',
    dimension_attributes=['name'],
    contract={'enforced': true},
) }}

select customer_id, effective_at, name from {{ ref('customer_observations') }}
where observation_batch = 1
"""

models__contract_dim_yml = """
version: 2

models:
  - name: customer_dim_contract
    config:
      contract:
        enforced: true
    columns:
      - name: customer_id
        data_type: integer
      - name: name
        data_type: varchar
      - name: valid_from
        data_type: date
      - name: valid_to
        data_type: date
      - name: dimension_version_id
        data_type: varchar
"""


@pytest.mark.skip_profile("md", "buenavista")
class TestDimensionBackfillContract:
    """The contract describes the history the model produces, not the observations."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"customer_observations.csv": seeds__customer_observations_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "customer_dim_contract.sql": models__contract_dim_sql,
            "customer_dim_contract.yml": models__contract_dim_yml,
        }

    def test_contract_columns_come_from_the_history(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        run_dbt(["run"])

        relation = relation_from_name(project.adapter, "customer_dim_contract")
        columns = [row[0] for row in project.run_sql(f"describe {relation}", fetch="all")]
        assert columns == [
            "customer_id",
            "name",
            "valid_from",
            "valid_to",
            "dimension_version_id",
        ]
