from loader import SQLSchemaLoader


loader = SQLSchemaLoader()
db = loader.load_from_file("estrutura.sql.txt")

print("Banco carregado:", db.name)
print("Tabelas encontradas:")
for table_name in db.tables:
    print("-", table_name)

TipoCliente = db.models["TipoCliente"]
Cliente = db.models["Cliente"]
Categoria = db.models["Categoria"]
Produto = db.models["Produto"]
Status = db.models["Status"]
Pedido = db.models["Pedido"]
PedidoProduto = db.models["Pedido_has_Produto"]

db.insert(TipoCliente(idTipoCliente=1, Descricao="Comum"))
db.insert(Cliente(idCliente=1, Nome="João", Email="joao@email.com", TipoCliente_idTipoCliente=1))
db.insert(Categoria(idCategoria=1, Descricao="Informática"))
db.insert(Produto(idProduto=1, Nome="Mouse Gamer", Descricao="Mouse RGB", Preco=150, QuantEstoque=10, Categoria_idCategoria=1))
db.insert(Status(idStatus=1, Descricao="Aberto"))
db.insert(Pedido(idPedido=1, Status_idStatus=1, Cliente_idCliente=1, ValorTotalPedido=300))
db.insert(PedidoProduto(Pedido_idPedido=1, Produto_idProduto=1, Quantidade=2, PrecoUnitario=150))

print()
print("Clientes:")
for item in db.get_table("Cliente").all():
    print(item)

print()
print("Produtos:")
for item in db.get_table("Produto").all():
    print(item)

print()
print("Pedidos:")
for item in db.get_table("Pedido").all():
    print(item)

print()
print("Itens do Pedido:")
for item in db.get_table("Pedido_has_Produto").all():
    print(item)