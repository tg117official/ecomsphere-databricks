"""Validate runtime inputs and centralize safe identifiers and SQL helpers."""
import re
from dataclasses import dataclass
from .fixtures import UPPER


def literal(value):
    """Quote trusted fixture/config literals; identifiers are validated separately."""
    if value is None:
        return 'NULL'
    # Fixtures contain no quotes or backslashes. Reject rather than interpolate arbitrary SQL.
    value = str(value)
    if "'" in value or '\\' in value:
        raise ValueError('Unsupported character in lab SQL literal')
    return "'" + value + "'"


@dataclass
class Context:
    spark: object
    dbutils: object
    catalog: str
    prefix: str
    run_id: str
    batch: int

    def __post_init__(self):
        # Restrict all DDL and volume operations to a named exercise namespace.
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', self.catalog):
            raise ValueError('Use an existing catalog with a simple SQL identifier')
        if not re.fullmatch(r'ecomsphere17_[a-z0-9_]+', self.prefix):
            raise ValueError('Lab prefix must start with ecomsphere17_')
        if not re.fullmatch(r'[0-9]+', self.run_id):
            raise ValueError('Run via the deployed job; expected a numeric job run ID')
        if self.batch not in UPPER:
            raise ValueError('Batch must be 1, 2 or 3')
        self.spark.conf.set('spark.sql.session.timeZone', 'UTC')

    def schema(self, layer):
        return f'`{self.catalog}`.`{self.prefix}_{layer}`'

    def table(self, layer, name):
        return f'{self.schema(layer)}.`{name}`'

    def volume(self, name):
        return f'/Volumes/{self.catalog}/{self.prefix}_source/{name}'

    @property
    def upper(self):
        return f"TIMESTAMP '{UPPER[self.batch]}'"

    @property
    def run(self):
        return literal(self.run_id)

    def sql(self, query):
        return self.spark.sql(query)

    def exists(self, layer, name):
        # SHOW TABLES works without relying on driver-side catalog internals.
        schemas = {r[0].lower() for r in self.sql(f"SHOW SCHEMAS IN `{self.catalog}`").collect()}
        if f"{self.prefix}_{layer}" not in schemas:
            return False
        return any(r[1] == name and not r[2] for r in self.sql(f"SHOW TABLES IN {self.schema(layer)}").collect())

    def scalar(self, query):
        return self.sql(query).first()[0]

    def replace(self, layer, name, query):
        # Rebuild a small deterministic current-state projection atomically per Delta table.
        self.sql(f'CREATE OR REPLACE TABLE {self.table(layer,name)} USING DELTA AS {query}')

    def create(self, layer, name, ddl):
        self.sql(f'CREATE TABLE IF NOT EXISTS {self.table(layer,name)} ({ddl}) USING DELTA')
