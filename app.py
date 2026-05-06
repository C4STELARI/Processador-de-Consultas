import streamlit as st
import copy
import graphviz
from loader import SQLSchemaLoader
from parser import SQLParser
from tree import (
    build_canonical_tree,
    generate_execution_plan,
    optimize_tree,
    apply_projection_heuristic,
)

st.set_page_config(page_title="Otimizador de Consultas SQL", layout="wide")

st.title("Processador de Consultas e Otimizador")

st.sidebar.markdown("---")


def color_box(color, text):
    return f"""
    <div style="display: flex; align-items: center; margin-bottom: 10px;">
        <div style="width: 20px; height: 20px; background-color: {color}; border-radius: 4px; border: 1px solid #999; margin-right: 10px;"></div>
        <span>{text}</span>
    </div>
    """


st.sidebar.markdown(
    color_box("#C8E6C9", "<b>Tabela</b> (Relação Base)"), unsafe_allow_html=True
)
st.sidebar.markdown(
    color_box("#FFF9C4", "<b>Seleção</b> (Filtro σ)"), unsafe_allow_html=True
)
st.sidebar.markdown(
    color_box("#E3F2FD", "<b>Projeção</b> (Atributos π)"), unsafe_allow_html=True
)
st.sidebar.markdown(
    color_box("#FFCCBC", "<b>Junção</b> (Join ⋈)"), unsafe_allow_html=True
)


def generate_graph(node, dot=None):
    is_root = False
    if dot is None:
        dot = graphviz.Digraph()
        dot.attr(rankdir="BT")
        dot.attr("node", fontname="Arial")
        is_root = True

    node_id = str(id(node))

    colors = {"Table": "#C8E6C9", "Selection": "#FFF9C4", "Join": "#FFCCBC"}
    color = colors.get(node.type_node, "#E3F2FD")

    dot.node(
        node_id,
        node.name,
        style="filled, rounded",
        fillcolor=color,
        shape="box",
        margin="0.2",
    )

    for child in node.children:
        generate_graph(child, dot)
        dot.edge(str(id(child)), node_id)

    return dot if is_root else node_id


st.sidebar.header("Configurações")
try:
    loader = SQLSchemaLoader()
    db = loader.load_from_file("estrutura.sql.txt")
    st.sidebar.success(f"Banco '{db.name}' carregado!")
    st.sidebar.write(f"Tabelas: {', '.join(db.tables.keys())}")
except Exception as e:
    st.sidebar.error(f"Erro ao carregar banco: {e}")

query_input = st.text_area(
    "Digite sua consulta SQL:",
    "SELECT Cliente.Nome, Pedido.idPedido FROM Cliente INNER JOIN Pedido ON Cliente.idCliente = Pedido.Cliente_idCliente WHERE Pedido.ValorTotalPedido < 500",
)

if st.button("Analisar e Otimizar"):
    parser = SQLParser(db)
    resultado = parser.analyze(query_input)

    if resultado["success"]:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("1. Árvore Canônica")
            arvore_canonica = build_canonical_tree(resultado["parsed"])
            grafico_canonico = generate_graph(arvore_canonica)
            st.graphviz_chart(grafico_canonico)

        with col2:
            st.subheader("2. Árvore Otimizada")
            arvore_otimizada = apply_projection_heuristic(
                copy.deepcopy(arvore_canonica)
            )
            arvore_final = optimize_tree(arvore_otimizada)

            grafico_otimizado = generate_graph(arvore_final)
            st.graphviz_chart(grafico_otimizado)

        st.success("Otimização concluída!")

        st.divider()
        st.subheader("Plano de Execução Sugerido")

        plano = generate_execution_plan(arvore_final)

        for i, passo in enumerate(plano, 1):
            st.write(f"**Passo {i}:** {passo}")

    else:
        st.error("Erro na consulta:")
        for err in resultado["errors"]:
            st.write(f"- {err}")

# SELECT Produto.Nome, Categoria.Descricao FROM Categoria INNER JOIN Produto ON Categoria.idCategoria = Produto.Categoria_idCategoria INNER JOIN Pedido_has_Produto ON Produto.idProduto = Pedido_has_Produto.Produto_idProduto  WHERE Produto.Preco > 100.00
