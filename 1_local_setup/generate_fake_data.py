import os
import uuid
import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd

# Inicializa o Faker configurado para o padrão brasileiro
fake = Faker('pt_BR')

# Configurações de saída
PASTA_SAIDA = "./dados_gerados"
os.makedirs(PASTA_SAIDA, exist_ok=True)

def gerar_cadastros_e_pedidos(total_clientes=20000, total_pedidos=50000):
    """Gera dados falsos de clientes e pedidos em lotes e os salva em arquivos CSV."""
    chunk_size = 10000
    
    # -------------------------------------------------------------
    # 1. GERAÇÃO DE CADASTROS (CLIENTES)
    # -------------------------------------------------------------
    print(f"Gerando {total_clientes} registros de cadastros...")
    chunks_cadastros = []
    lista_ids_clientes = [] # Guardará os IDs para linkar com os pedidos
    
    for chunk_start in range(0, total_clientes, chunk_size):
        chunk_end = min(chunk_start + chunk_size, total_clientes)
        chunk_data = []
        
        for _ in range(chunk_start, chunk_end):
            id_cliente = str(uuid.uuid4())
            lista_ids_clientes.append(id_cliente)
            
            chunk_data.append({
                'id_cliente': id_cliente,
                'nome': fake.name(),
                'data_nascimento': fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
                'cpf': fake.bothify(text='###.###.###-##'),
                'estado': fake.state_abbr(),
                'data_cadastro': fake.date_between(start_date='-2y', end_date='today').isoformat()
            })
        
        chunks_cadastros.append(pd.DataFrame(chunk_data))
        
    df_cadastros = pd.concat(chunks_cadastros, ignore_index=True)
    caminho_cadastros = os.path.join(PASTA_SAIDA, "cadastros.csv")
    df_cadastros.to_csv(caminho_cadastros, index=False)
    print(f"-> Arquivo de cadastros salvo em: {caminho_cadastros}")

    # -------------------------------------------------------------
    # 2. GERAÇÃO DE PEDIDOS (CONECTADOS AOS CLIENTES)
    # -------------------------------------------------------------
    print(f"Gerando {total_pedidos} registros de pedidos...")
    chunks_pedidos = []
    
    # Listas auxiliares para dados dos pedidos
    produtos = ["Notebook", "Smartphone", "Monitor 4K", "Teclado Mecânico", "Mouse Gamer", "Fone Bluetooth"]
    status_opcoes = ["Entregue", "Processando", "Cancelado", "Aguardando Pagamento"]
    
    for chunk_start in range(0, total_pedidos, chunk_size):
        chunk_end = min(chunk_start + chunk_size, total_pedidos)
        chunk_data = []
        
        for _ in range(chunk_start, chunk_end):
            quantidade = random.randint(1, 5)
            preco_unitario = round(random.uniform(40.0, 3500.0), 2)
            
            chunk_data.append({
                'id_pedido': str(uuid.uuid4()),
                # Sorteia um ID de cliente existente para manter a integridade referencial
                'id_cliente': random.choice(lista_ids_clientes), 
                'produto': random.choice(produtos),
                'quantidade': quantidade,
                'preco_unitario': preco_unitario,
                'valor_total': round(quantidade * preco_unitario, 2),
                'data_pedido': fake.date_between(start_date='-1y', end_date='today').isoformat(),
                'status': random.choice(status_opcoes)
            })
            
        chunks_pedidos.append(pd.DataFrame(chunk_data))
        
    df_pedidos = pd.concat(chunks_pedidos, ignore_index=True)
    caminho_pedidos = os.path.join(PASTA_SAIDA, "pedidos.csv")
    df_pedidos.to_csv(caminho_pedidos, index=False)
    print(f"-> Arquivo de pedidos salvo em: {caminho_pedidos}")

if __name__ == "__main__":
    # Altere os valores abaixo se quiser gerar mais ou menos linhas
    gerar_cadastros_e_pedidos(total_clientes=20000, total_pedidos=50000)
    print("\nProcesso concluído com sucesso!")
