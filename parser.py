import re
from dataclasses import dataclass, asdict


class SQLParseError(Exception):
    pass


@dataclass
class TableRef:
    name: str
    alias: str


@dataclass
class FieldRef:
    raw: str
    table_alias: str | None
    column: str


@dataclass
class Comparison:
    left: str
    operator: str
    right: str
    raw: str


@dataclass
class JoinRef:
    table: TableRef
    condition: Comparison


@dataclass
class ParsedQuery:
    query_type: str
    select_fields: list
    from_table: TableRef
    joins: list
    where: str | None
    where_conditions: list

    def to_dict(self):
        return {
            "query_type": self.query_type,
            "select_fields": [asdict(field) for field in self.select_fields],
            "from_table": asdict(self.from_table),
            "joins": [
                {
                    "table": asdict(join.table),
                    "condition": asdict(join.condition),
                }
                for join in self.joins
            ],
            "where": self.where,
            "where_conditions": [asdict(condition) for condition in self.where_conditions],
        }


class SQLParser:
    SUPPORTED_COMMANDS = {"SELECT"}
    COMPARISON_OPERATORS = {"=", ">", "<", ">=", "<=", "<>"}
    RESERVED_CLAUSES = ("SELECT", "FROM", "WHERE", "INNER JOIN", "GROUP BY", "ORDER BY", "HAVING")

    def __init__(self, database):
        self.database = database

    def analyze(self, query: str):
        query = self._normalize_sql(query)
        try:
            parsed = self.parse(query)
            errors = self.validate(parsed)
            return {
                "success": len(errors) == 0,
                "parsed": parsed.to_dict(),
                "errors": errors,
            }
        except SQLParseError as exc:
            return {
                "success": False,
                "parsed": None,
                "errors": [str(exc)],
            }

    def parse(self, query: str) -> ParsedQuery:
        if not query:
            raise SQLParseError("A consulta SQL está vazia")

        command = self._extract_command(query)
        if command not in self.SUPPORTED_COMMANDS:
            raise SQLParseError(f"Comando não suportado: {command}")

        if not re.match(r"^SELECT\b", query, re.IGNORECASE):
            raise SQLParseError("A consulta deve começar com SELECT")

        select_match = re.search(r"^SELECT\s+(.*?)\s+FROM\s+", query, re.IGNORECASE | re.DOTALL)
        if not select_match:
            raise SQLParseError("Não foi possível identificar as cláusulas SELECT e FROM")

        fields_text = select_match.group(1).strip()
        tail = query[select_match.end():].strip()
        if not tail:
            raise SQLParseError("A cláusula FROM está incompleta")

        from_table, remainder = self._parse_table_reference(tail)
        joins, remainder = self._parse_joins(remainder)
        where_text = self._parse_where_clause(remainder)

        return ParsedQuery(
            query_type="SELECT",
            select_fields=self._parse_select_fields(fields_text),
            from_table=from_table,
            joins=joins,
            where=where_text,
            where_conditions=self._parse_where_conditions(where_text),
        )

    def validate(self, parsed: ParsedQuery):
        errors = []
        alias_map = {}

        errors.extend(self._validate_table_ref(parsed.from_table, alias_map))
        for join in parsed.joins:
            errors.extend(self._validate_table_ref(join.table, alias_map))

        errors.extend(self._validate_select_fields(parsed.select_fields, alias_map))

        for join in parsed.joins:
            errors.extend(self._validate_comparison(join.condition, alias_map, context="JOIN"))

        for condition in parsed.where_conditions:
            errors.extend(self._validate_comparison(condition, alias_map, context="WHERE"))

        return errors

    def _normalize_sql(self, query: str):
        query = query.strip().rstrip(";")
        query = re.sub(r"\s+", " ", query)
        return query

    def _extract_command(self, query: str):
        match = re.match(r"^([A-Za-z]+)", query)
        if not match:
            raise SQLParseError("Não foi possível identificar o comando SQL")
        return match.group(1).upper()

    def _parse_select_fields(self, fields_text: str):
        if not fields_text:
            raise SQLParseError("A cláusula SELECT está vazia")

        fields = []
        for raw_field in [field.strip() for field in fields_text.split(",")]:
            if not raw_field:
                raise SQLParseError("Há um campo vazio na cláusula SELECT")

            if raw_field == "*":
                fields.append(FieldRef(raw=raw_field, table_alias=None, column="*"))
                continue

            qualified_star = re.match(r"^([A-Za-z_]\w*)\.\*$", raw_field)
            if qualified_star:
                fields.append(FieldRef(raw=raw_field, table_alias=qualified_star.group(1), column="*"))
                continue

            qualified = re.match(r"^([A-Za-z_]\w*)\.([A-Za-z_]\w*)$", raw_field)
            if qualified:
                fields.append(FieldRef(raw=raw_field, table_alias=qualified.group(1), column=qualified.group(2)))
                continue

            simple = re.match(r"^([A-Za-z_]\w*)$", raw_field)
            if simple:
                fields.append(FieldRef(raw=raw_field, table_alias=None, column=simple.group(1)))
                continue

            raise SQLParseError(f"Campo inválido no SELECT: {raw_field}")

        return fields

    def _parse_table_reference(self, text: str):
        pattern = re.compile(r"^([A-Za-z_]\w*)(?:\s+(?:AS\s+)?([A-Za-z_]\w*))?(.*)$", re.IGNORECASE)
        match = pattern.match(text)
        if not match:
            raise SQLParseError("Tabela inválida na cláusula FROM")

        table_name = match.group(1)
        alias = match.group(2) or table_name
        remainder = match.group(3).strip()

        if alias.upper() in {"INNER", "WHERE"}:
            alias = table_name
            remainder = text[len(table_name):].strip()

        return TableRef(name=table_name, alias=alias), remainder

    def _parse_joins(self, remainder: str):
        joins = []
        join_pattern = re.compile(
            r"^INNER\s+JOIN\s+([A-Za-z_]\w*)(?:\s+(?:AS\s+)?([A-Za-z_]\w*))?\s+ON\s+(.+?)(?=(?:\s+INNER\s+JOIN\s+|\s+WHERE\s+|$))",
            re.IGNORECASE,
        )

        while remainder:
            join_match = join_pattern.match(remainder)
            if not join_match:
                break

            table_name = join_match.group(1)
            alias = join_match.group(2) or table_name
            condition_text = join_match.group(3).strip()
            comparison = self._parse_single_comparison(condition_text, context="JOIN")
            joins.append(JoinRef(table=TableRef(name=table_name, alias=alias), condition=comparison))
            remainder = remainder[join_match.end():].strip()

        return joins, remainder

    def _parse_where_clause(self, remainder: str):
        if not remainder:
            return None

        where_match = re.match(r"^WHERE\s+(.+)$", remainder, re.IGNORECASE)
        if where_match:
            return where_match.group(1).strip()

        unsupported_clause = self._find_unsupported_clause(remainder)
        if unsupported_clause:
            raise SQLParseError(f"Cláusula não suportada nesta etapa: {unsupported_clause}")

        raise SQLParseError(f"Trecho SQL não reconhecido: {remainder}")

    def _find_unsupported_clause(self, text: str):
        upper = text.upper()
        for clause in self.RESERVED_CLAUSES:
            if clause in upper:
                return clause
        return None

    def _parse_where_conditions(self, where_text: str):
        if not where_text:
            return []

        cleaned = where_text.replace("(", " ").replace(")", " ").strip()
        if not cleaned:
            raise SQLParseError("A cláusula WHERE está vazia")

        parts = [part.strip() for part in re.split(r"\bAND\b", cleaned, flags=re.IGNORECASE)]
        conditions = []
        for part in parts:
            if not part:
                raise SQLParseError("Condição inválida na cláusula WHERE")
            conditions.append(self._parse_single_comparison(part, context="WHERE"))

        self._validate_parentheses(where_text)
        return conditions

    def _validate_parentheses(self, text: str):
        balance = 0
        for char in text:
            if char == "(":
                balance += 1
            elif char == ")":
                balance -= 1
            if balance < 0:
                raise SQLParseError("Parênteses desbalanceados na cláusula WHERE")
        if balance != 0:
            raise SQLParseError("Parênteses desbalanceados na cláusula WHERE")

    def _parse_single_comparison(self, text: str, context: str):
        comparison_pattern = re.compile(r"^(.+?)\s*(<=|>=|<>|=|<|>)\s*(.+)$")
        match = comparison_pattern.match(text.strip())
        if not match:
            raise SQLParseError(f"Condição inválida no {context}: {text}")

        left = match.group(1).strip()
        operator = match.group(2).strip()
        right = match.group(3).strip()

        if operator not in self.COMPARISON_OPERATORS:
            raise SQLParseError(f"Operador não suportado: {operator}")

        return Comparison(left=left, operator=operator, right=right, raw=text.strip())

    def _validate_table_ref(self, table_ref: TableRef, alias_map: dict):
        errors = []
        if table_ref.name not in self.database.tables:
            errors.append(f"Tabela inexistente: {table_ref.name}")
            return errors

        normalized_alias = table_ref.alias
        if normalized_alias in alias_map:
            errors.append(f"Alias duplicado: {normalized_alias}")
        else:
            alias_map[normalized_alias] = table_ref.name

        return errors

    def _validate_select_fields(self, fields: list, alias_map: dict):
        errors = []
        for field in fields:
            if field.column == "*":
                if field.table_alias and field.table_alias not in alias_map:
                    errors.append(f"Alias desconhecido no SELECT: {field.table_alias}")
                continue

            error = self._resolve_column_reference(field.table_alias, field.column, alias_map, context="SELECT")
            if error:
                errors.append(error)
        return errors

    def _validate_comparison(self, comparison: Comparison, alias_map: dict, context: str):
        errors = []

        left_error = self._validate_operand(comparison.left, alias_map, context)
        right_error = self._validate_operand(comparison.right, alias_map, context)

        if left_error:
            errors.append(left_error)
        if right_error:
            errors.append(right_error)

        return errors

    def _validate_operand(self, operand: str, alias_map: dict, context: str):
        operand = operand.strip()
        if self._is_literal(operand):
            return None

        qualified = re.match(r"^([A-Za-z_]\w*)\.([A-Za-z_]\w*)$", operand)
        if qualified:
            return self._resolve_column_reference(qualified.group(1), qualified.group(2), alias_map, context)

        simple = re.match(r"^([A-Za-z_]\w*)$", operand)
        if simple:
            return self._resolve_column_reference(None, simple.group(1), alias_map, context)

        return f"Operando inválido no {context}: {operand}"

    def _resolve_column_reference(self, table_alias: str | None, column_name: str, alias_map: dict, context: str):
        if table_alias:
            if table_alias not in alias_map:
                return f"Alias desconhecido no {context}: {table_alias}"

            table_name = alias_map[table_alias]
            table = self.database.tables[table_name]
            if column_name not in table.columns:
                return f"Campo inexistente em {table_name}: {column_name}"
            return None

        matches = []
        for alias, table_name in alias_map.items():
            table = self.database.tables[table_name]
            if column_name in table.columns:
                matches.append((alias, table_name))

        if not matches:
            return f"Campo inexistente: {column_name}"

        unique_tables = {table_name for _, table_name in matches}
        if len(unique_tables) > 1:
            tables_str = ", ".join(sorted(unique_tables))
            return f"Campo ambíguo no {context}: {column_name}. Use o nome da tabela/alias ({tables_str})"

        return None

    def _is_literal(self, value: str):
        if re.fullmatch(r"-?\d+(?:\.\d+)?", value):
            return True
        if re.fullmatch(r"'(?:[^']*)'", value):
            return True
        if re.fullmatch(r'"(?:[^"]*)"', value):
            return True
        return False
