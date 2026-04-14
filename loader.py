import re
from pathlib import Path
from database import Column, ForeignKey, Table, InMemoryDatabase


class SQLSchemaLoader:
    def load_from_file(self, file_path):
        text = Path(file_path).read_text(encoding="utf-8")
        return self.load_from_text(text)

    def load_from_text(self, text):
        db_name = self.extract_database_name(text)
        database = InMemoryDatabase(db_name)

        create_tables = self.extract_create_tables(text)
        for table_name, body in create_tables:
            table = Table(table_name)
            parts = self.split_by_comma(body)

            for part in parts:
                item = part.strip()
                upper_item = item.upper()

                if upper_item.startswith("PRIMARY KEY"):
                    pk_columns = self.extract_columns_from_parentheses(item)
                    table.set_primary_key(pk_columns)
                elif upper_item.startswith("CONSTRAINT") or upper_item.startswith("FOREIGN KEY"):
                    pass
                else:
                    column = self.parse_column(item)
                    table.add_column(column)

            database.add_table(table)

        alter_tables = self.extract_alter_tables(text)
        for table_name, body in alter_tables:
            foreign_keys = self.parse_foreign_keys(body)
            for fk in foreign_keys:
                database.tables[table_name].add_foreign_key(fk)

        database.build_models()
        return database

    def extract_database_name(self, text):
        match = re.search(r"USE\s+([A-Za-z_]\w*)", text, re.IGNORECASE)
        if match:
            return match.group(1)
        return "BancoMemoria"

    def extract_create_tables(self, text):
        pattern = re.compile(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_]\w*)\s*\(", re.IGNORECASE)
        blocks = []

        for match in pattern.finditer(text):
            table_name = match.group(1)
            start = match.end() - 1
            end = self.find_closing_parenthesis(text, start)
            body = text[start + 1:end]
            blocks.append((table_name, body))

        return blocks

    def extract_alter_tables(self, text):
        pattern = re.compile(r"ALTER\s+TABLE\s+([A-Za-z_]\w*)\s+(.*?);", re.IGNORECASE | re.DOTALL)
        return [(match.group(1), match.group(2)) for match in pattern.finditer(text)]

    def find_closing_parenthesis(self, text, start_pos):
        level = 0
        for i in range(start_pos, len(text)):
            if text[i] == "(":
                level += 1
            elif text[i] == ")":
                level -= 1
                if level == 0:
                    return i
        raise ValueError("Parênteses não balanceados")

    def split_by_comma(self, text):
        items = []
        current = []
        level = 0

        for char in text:
            if char == "(":
                level += 1
            elif char == ")":
                level -= 1

            if char == "," and level == 0:
                items.append("".join(current))
                current = []
            else:
                current.append(char)

        if current:
            items.append("".join(current))

        return items

    def parse_column(self, item):
        match = re.match(
            r"([A-Za-z_]\w*)\s+([A-Za-z]+(?:\(\d+(?:,\d+)?\))?)\s*(.*)",
            item,
            re.IGNORECASE | re.DOTALL
        )

        if not match:
            raise ValueError(f"Não foi possível ler a coluna: {item}")

        name = match.group(1)
        sql_type = match.group(2)
        extras = match.group(3)

        nullable = "NOT NULL" not in extras.upper()
        auto_increment = "AUTO_INCREMENT" in extras.upper()

        default_match = re.search(r"DEFAULT\s+([^\s,]+(?:\([^)]+\))?)", extras, re.IGNORECASE)
        default = default_match.group(1) if default_match else None

        return Column(
            name=name,
            sql_type=sql_type,
            nullable=nullable,
            default=default,
            auto_increment=auto_increment
        )

    def extract_columns_from_parentheses(self, text):
        match = re.search(r"\((.*?)\)", text, re.DOTALL)
        if not match:
            return []
        return [col.strip() for col in match.group(1).split(",")]

    def parse_foreign_keys(self, body):
        pattern = re.compile(
            r"ADD\s+CONSTRAINT\s+([A-Za-z_]\w*)\s+FOREIGN\s+KEY\s*\((.*?)\)\s+REFERENCES\s+([A-Za-z_]\w*)\s*\((.*?)\)",
            re.IGNORECASE | re.DOTALL
        )

        foreign_keys = []

        for match in pattern.finditer(body):
            name = match.group(1)
            local_columns = [col.strip() for col in match.group(2).split(",")]
            referenced_table = match.group(3)
            referenced_columns = [col.strip() for col in match.group(4).split(",")]

            foreign_keys.append(
                ForeignKey(
                    name=name,
                    local_columns=local_columns,
                    referenced_table=referenced_table,
                    referenced_columns=referenced_columns
                )
            )

        return foreign_keys