# tree.py
from dataclasses import dataclass
import re


class Node:
    def __init__(self, name, type_node):
        self.name = name
        self.type_node = type_node
        self.children = []

    def add_child(self, child_node):
        self.children.append(child_node)


class ProjectionNode(Node):
    def __init__(self, columns, child=None):
        super().__init__(f"π ({', '.join(columns)})", "Projection")
        if child:
            self.add_child(child)


class SelectionNode(Node):
    def __init__(self, condition, child=None):
        super().__init__(f"σ ({condition})", "Selection")
        if child:
            self.add_child(child)


class JoinNode(Node):
    def __init__(self, condition, left_child, right_child):
        super().__init__(f"⋈ ({condition})", "Join")
        self.add_child(left_child)
        self.add_child(right_child)


class TableNode(Node):
    def __init__(self, table_name):
        super().__init__(table_name, "Table")


def build_canonical_tree(parsed_query):

    data = parsed_query

    root = TableNode(data["from_table"]["name"])

    for join in data["joins"]:
        right_child = TableNode(join["table"]["name"])
        root = JoinNode(join["condition"]["raw"], root, right_child)

    if data["where"]:
        root = SelectionNode(data["where"], root)

    cols = [f["column"] for f in data["select_fields"]]
    root = ProjectionNode(cols, root)

    return root


def print_tree(node, level=0):
    indent = "  " * level
    print(f"{indent}[{node.type_node}] {node.name}")
    for child in node.children:
        print_tree(child, level + 1)


def apply_projection_heuristic(node, required_columns=None):
    if required_columns is None:
        required_columns = set()

    if node.type_node == "Projection":
        cols = node.name.replace("PI (", "").replace(")", "").split(", ")
        required_columns.update(cols)
        node.children[0] = apply_projection_heuristic(
            node.children[0], required_columns
        )
        return node

    if node.type_node == "Selection":
        potential_cols = re.findall(r"([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)", node.name)
        required_columns.update(potential_cols)
        node.children[0] = apply_projection_heuristic(
            node.children[0], required_columns
        )
        return node

    if node.type_node == "Join":
        potential_cols = re.findall(r"([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)", node.name)
        required_columns.update(potential_cols)
        node.children[0] = apply_projection_heuristic(
            node.children[0], required_columns
        )
        node.children[1] = apply_projection_heuristic(
            node.children[1], required_columns
        )
        return node

    if node.type_node == "Table":
        table_name = node.name
        cols_for_this_table = []
        for col in required_columns:
            if "." in col:
                t_prefix, c_name = col.split(".")
                if t_prefix == table_name:
                    cols_for_this_table.append(c_name)
            else:
                cols_for_this_table.append(col)

        if cols_for_this_table:
            new_proj = ProjectionNode(list(set(cols_for_this_table)), node)
            return new_proj

    return node


def optimize_tree(node):
    if node is None:
        return None

    node.children = [optimize_tree(child) for child in node.children]

    if node.type_node == "Selection":
        child = node.children[0]
        condition = node.name.lower()

        def has_table(root, name):
            if root.type_node == "Table" and root.name.lower() in name:
                return True
            return any(has_table(c, name) for c in root.children)

        if child.type_node == "Projection":
            node.children = [child.children[0]]
            child.children[0] = optimize_tree(node)
            return child

        if child.type_node == "Join":
            left_branch = child.children[0]
            right_branch = child.children[1]

            if has_table(left_branch, condition):
                node.children = [left_branch]
                child.children[0] = optimize_tree(node)
                return child
            elif has_table(right_branch, condition):
                node.children = [right_branch]
                child.children[1] = optimize_tree(node)
                return child

    return node


def generate_execution_plan(node, steps=None):
    if steps is None:
        steps = []

    for child in node.children:
        generate_execution_plan(child, steps)

    if node.type_node == "Table":
        steps.append(f"Acessar a tabela '{node.name}'.")
    elif node.type_node == "Selection":
        steps.append(f"Aplicar filtro de seleção: {node.name}.")
    elif node.type_node == "Projection":
        steps.append(f"Restringir colunas (projeção): {node.name}.")
    elif node.type_node == "Join":
        steps.append(f"Realizar junção (JOIN) com a condição: {node.name}.")

    return steps
