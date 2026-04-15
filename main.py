from pprint import pprint
from loader import SQLSchemaLoader
from parser import SQLParser


loader = SQLSchemaLoader()
db = loader.load_from_file("estrutura.sql.txt")
parser = SQLParser(db)

print("Banco carregado:", db.name)
print("Tabelas encontradas:")
for table_name in db.tables:
    print("-", table_name)

consultas = [
    "SELECT Nome, Email FROM Cliente WHERE idCliente > 10",
    "SELECT Cliente.Nome, Pedido.idPedido FROM Cliente INNER JOIN Pedido ON Cliente.idCliente = Pedido.Cliente_idCliente WHERE Pedido.ValorTotalPedido >= 100",
    "SELECT CPF FROM Cliente",
    "SELEC Nome FROM Cliente",
]

for consulta in consultas:
    print("\nConsulta:", consulta)
    resultado = parser.analyze(consulta)
    pprint(resultado)
