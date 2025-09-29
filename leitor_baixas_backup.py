import pandas as pd

# Caminho do arquivo
file_path = "relação de documentos baixados 10.09.25.csv"

# Ler como texto cru
with open(file_path, "r", encoding="latin1") as f:
    linhas = f.readlines()

# Lista final de registros
registros = []

# Variável de controle para o Centro de Resultados
centro_atual = None

# Loop pelas linhas, ignorando as 5 primeiras
for linha in linhas[5:]:
    linha = linha.strip()
    if not linha:  # ignora linhas vazias
        continue

    partes = linha.split(";")
    primeira_col = partes[0].strip()

    # Regras para ignorar linhas
    if primeira_col == "" or \
       primeira_col.startswith("Data") or \
       primeira_col.startswith("Subtotal") or \
       primeira_col.startswith("Sistema Posto Delta") or \
       primeira_col.startswith("Total"):
        continue

    # Detectar Centro de Resultados (só a coluna A preenchida)
    if primeira_col and all(p == "" for p in partes[1:]):
        centro_atual = primeira_col
        continue

    # Detectar linha de dados (começa com data dd/mm/aaaa)
    if "/" in primeira_col:
        # Campos fixos
        data = partes[0]
        lancamento = partes[1] if len(partes) > 1 else None
        conta = partes[2] if len(partes) > 2 else None

        documento = None
        valor_total = None
        data_baixa = None
        lancamento_baixa = None
        responsavel = None

        # Regras de mapeamento conforme posição do "Lançamento Baixa"
        if len(partes) > 10 and partes[10] != "":  # Coluna K
            lancamento_baixa = partes[10]
            responsavel = partes[3]  # Coluna D
            documento = partes[5]
            valor_total = partes[6]
            data_baixa = partes[8]

        elif len(partes) > 11 and partes[11] != "":  # Coluna L
            lancamento_baixa = partes[11]
            responsavel = partes[4]  # Coluna E
            documento = partes[6]
            valor_total = partes[7]
            data_baixa = partes[9]

        elif len(partes) > 13 and partes[13] != "":  # Coluna N
            lancamento_baixa = partes[13]
            responsavel = partes[3]  # Coluna D
            documento = partes[6]
            valor_total = partes[7]
            data_baixa = partes[9]

        # Montar registro
        registro = {
            "Centro de Resultados": centro_atual,
            "Data": data,
            "Lançamento": lancamento,
            "Conta": conta,
            "Responsável": responsavel,
            "Documento": documento,
            "Valor Total": valor_total,
            "Data Baixa": data_baixa,
            "Lançamento Baixa": lancamento_baixa
        }

        registros.append(registro)

# Converter para DataFrame
df_final = pd.DataFrame(registros)

# -------------------------------
# Conversões de tipos
# -------------------------------
# Datas
df_final["Data"] = pd.to_datetime(df_final["Data"], format="%d/%m/%Y", errors="coerce")
df_final["Data Baixa"] = pd.to_datetime(df_final["Data Baixa"], format="%d/%m/%Y", errors="coerce")

# Valor Total para float
df_final["Valor Total"] = (
    df_final["Valor Total"]
    .astype(str)
    .str.replace(".", "", regex=False)   # remove separador de milhar
    .str.replace(",", ".", regex=False)  # troca vírgula decimal por ponto
)
df_final["Valor Total"] = pd.to_numeric(df_final["Valor Total"], errors="coerce")

# -------------------------------
# Exportar resultados
# -------------------------------
# CSV
df_final.to_csv("base_limpa.csv", index=False, encoding="utf-8-sig")

# Excel
df_final.to_excel("base_limpa.xlsx", index=False, engine="openpyxl")

print("✅ Processamento concluído! Registros finais:", len(df_final))
print("Arquivos gerados: base_limpa.csv e base_limpa.xlsx")
print(df_final.head())
