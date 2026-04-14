from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import re


@dataclass
class Column:
    name: str
    sql_type: str
    nullable: bool = True
    default: object = None
    auto_increment: bool = False


@dataclass
class ForeignKey:
    name: str
    local_columns: list
    referenced_table: str
    referenced_columns: list


class RowModel:
    __table_name__ = None
    __columns__ = ()

    def __init__(self, **kwargs):
        for col in self.__class__.__columns__:
            setattr(self, col, kwargs.get(col))

    def to_dict(self):
        return {col: getattr(self, col, None) for col in self.__class__.__columns__}

    def __repr__(self):
        valores = ", ".join(f"{col}={getattr(self, col, None)!r}" for col in self.__class__.__columns__)
        return f"{self.__class__.__name__}({valores})"


class Table:
    def __init__(self, name):
        self.name = name
        self.columns = {}
        self.primary_key = []
        self.foreign_keys = []
        self.rows = []
        self.auto_increment_values = {}

    def add_column(self, column):
        self.columns[column.name] = column
        if column.auto_increment:
            self.auto_increment_values[column.name] = 1

    def set_primary_key(self, columns):
        self.primary_key = columns

    def add_foreign_key(self, foreign_key):
        self.foreign_keys.append(foreign_key)

    def next_auto_increment(self, column_name):
        value = self.auto_increment_values[column_name]
        self.auto_increment_values[column_name] += 1
        return value

    def convert_value(self, value, sql_type):
        if value is None:
            return None

        base_type = sql_type.upper().split("(")[0]

        if base_type in ["INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"]:
            return int(value)

        if base_type in ["DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL"]:
            return Decimal(str(value))

        if base_type in ["DATETIME", "TIMESTAMP", "DATE"]:
            if isinstance(value, datetime):
                return value
            try:
                return datetime.fromisoformat(str(value))
            except ValueError:
                return value

        return str(value)

    def resolve_default(self, default):
        if default is None:
            return None

        value = str(default).strip().strip("'").strip('"')
        lowered = value.lower()

        if lowered in ["now()", "current_timestamp", "current_timestamp()"]:
            return datetime.now()

        if re.fullmatch(r"-?\d+", value):
            return int(value)

        if re.fullmatch(r"-?\d+\.\d+", value):
            return Decimal(value)

        return value

    def get_pk_value(self, row):
        if not self.primary_key:
            return None

        if len(self.primary_key) == 1:
            return row[self.primary_key[0]]

        return tuple(row[col] for col in self.primary_key)

    def exists_by_columns(self, columns, values):
        for row in self.rows:
            ok = True
            for col, val in zip(columns, values):
                if row.get(col) != val:
                    ok = False
                    break
            if ok:
                return True
        return False

    def insert(self, database, data):
        if isinstance(data, RowModel):
            data = data.to_dict()

        row = {}

        for column_name, column in self.columns.items():
            if column_name in data and data[column_name] is not None:
                value = data[column_name]
            elif column.auto_increment:
                value = self.next_auto_increment(column_name)
            else:
                value = self.resolve_default(column.default)

            value = self.convert_value(value, column.sql_type)

            if value is None and not column.nullable:
                raise ValueError(f"A coluna {self.name}.{column_name} não aceita NULL")

            row[column_name] = value

        new_pk = self.get_pk_value(row)
        if new_pk is not None:
            for existing_row in self.rows:
                if self.get_pk_value(existing_row) == new_pk:
                    raise ValueError(f"Chave primária duplicada na tabela {self.name}")

        for fk in self.foreign_keys:
            local_values = [row[col] for col in fk.local_columns]
            referenced_table = database.tables[fk.referenced_table]

            if not referenced_table.exists_by_columns(fk.referenced_columns, local_values):
                raise ValueError(
                    f"Erro de chave estrangeira em {self.name}: "
                    f"{fk.local_columns} -> {fk.referenced_table}.{fk.referenced_columns}"
                )

        self.rows.append(row)
        return row

    def all(self):
        return self.rows

    def filter(self, **kwargs):
        result = []
        for row in self.rows:
            ok = True
            for key, value in kwargs.items():
                if row.get(key) != value:
                    ok = False
                    break
            if ok:
                result.append(row)
        return result


class InMemoryDatabase:
    def __init__(self, name):
        self.name = name
        self.tables = {}
        self.models = {}

    def add_table(self, table):
        self.tables[table.name] = table

    def build_models(self):
        for table_name, table in self.tables.items():
            attrs = {
                "__table_name__": table_name,
                "__columns__": tuple(table.columns.keys())
            }
            self.models[table_name] = type(table_name, (RowModel,), attrs)

    def insert(self, table_name_or_model, data=None, **kwargs):
        if isinstance(table_name_or_model, RowModel):
            table_name = table_name_or_model.__class__.__table_name__
            return self.tables[table_name].insert(self, table_name_or_model)

        if isinstance(table_name_or_model, str):
            table_name = table_name_or_model
            payload = data if data is not None else kwargs
            return self.tables[table_name].insert(self, payload)

        raise TypeError("Informe o nome da tabela ou um objeto modelo")

    def get_table(self, name):
        return self.tables[name]