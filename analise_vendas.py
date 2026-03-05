"""
=============================================================
  ANÁLISE DE VENDAS GLOBAIS — Departamento de Vendas / TI
  Responde as 10 perguntas de negócio a partir dos CSVs
=============================================================
Uso:
    python analise_vendas.py

Arquivos esperados (mesmo diretório ou ajuste CAMINHO_BASE):
    VendasGlobais.csv
    Fornecedores.csv
    Transportadoras.csv
    Vendedores.csv
=============================================================
"""

import os
import pandas as pd

# ─────────────────────────────────────────────
#  CONFIGURAÇÃO — ajuste o caminho se necessário
# ─────────────────────────────────────────────
CAMINHO_BASE = "."          # pasta onde estão os CSVs

# Países considerados como Europa neste dataset
PAISES_EUROPA = {
    "Austria", "Belgium", "Denmark", "Finland", "France",
    "Germany", "Ireland", "Italy", "Norway", "Poland",
    "Portugal", "Spain", "Sweden", "Switzerland", "UK"
}

# ─────────────────────────────────────────────
#  CARREGAMENTO DOS DADOS
# ─────────────────────────────────────────────
def carregar_dados(base: str) -> pd.DataFrame:
    """Lê e integra todos os CSVs, retornando um DataFrame enriquecido."""
    vendas       = pd.read_csv(os.path.join(base, "VendasGlobais.csv"))
    fornecedores = pd.read_csv(os.path.join(base, "Fornecedores.csv"))
    transportad  = pd.read_csv(os.path.join(base, "Transportadoras.csv"))
    vendedores   = pd.read_csv(os.path.join(base, "Vendedores.csv"))

    # Normaliza datas e extrai ano
    vendas["Data"] = pd.to_datetime(vendas["Data"], dayfirst=True, errors="coerce")
    vendas["Ano"]  = vendas["Data"].dt.year

    # Joins com tabelas auxiliares
    vendas = vendas.merge(fornecedores, on="FornecedorID",    how="left")
    vendas = vendas.merge(transportad,  on="TransportadoraID", how="left")
    vendas = vendas.merge(vendedores,   on="VendedorID",       how="left")

    return vendas


# ─────────────────────────────────────────────
#  FUNÇÕES — 1 por questão
# ─────────────────────────────────────────────

def q1_top10_clientes(df: pd.DataFrame) -> pd.DataFrame:
    """Q1 — 10 maiores clientes em valor de vendas ($)."""
    resultado = (
        df.groupby("ClienteNome")["Vendas"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
        .rename(columns={"Vendas": "Total Vendas ($)"})
    )
    resultado.index = range(1, len(resultado) + 1)
    return resultado


def q2_top3_paises(df: pd.DataFrame) -> pd.DataFrame:
    """Q2 — 3 maiores países em valor de vendas ($)."""
    resultado = (
        df.groupby("ClientePaís")["Vendas"]
        .sum()
        .sort_values(ascending=False)
        .head(3)
        .reset_index()
        .rename(columns={"ClientePaís": "País", "Vendas": "Total Vendas ($)"})
    )
    resultado.index = range(1, len(resultado) + 1)
    return resultado


def q3_categorias_brasil(df: pd.DataFrame) -> pd.DataFrame:
    """Q3 — Categorias de produtos com maior faturamento no Brasil."""
    brasil = df[df["ClientePaís"] == "Brazil"]
    resultado = (
        brasil.groupby("CategoriaNome")["Vendas"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"CategoriaNome": "Categoria", "Vendas": "Faturamento ($)"})
    )
    resultado.index = range(1, len(resultado) + 1)
    return resultado


def q4_frete_por_transportadora(df: pd.DataFrame) -> pd.DataFrame:
    """Q4 — Despesa total de frete por transportadora."""
    resultado = (
        df.groupby("TransportadoraNome")["Frete"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"TransportadoraNome": "Transportadora", "Frete": "Total Frete ($)"})
    )
    resultado.index = range(1, len(resultado) + 1)
    return resultado


def q5_clientes_calcados_masc_alemanha(df: pd.DataFrame) -> pd.DataFrame:
    """Q5 — Principais clientes de 'Men´s Footwear' na Alemanha."""
    filtro = (
        (df["CategoriaNome"].str.strip() == "Men´s Footwear") &
        (df["ClientePaís"] == "Germany")
    )
    resultado = (
        df[filtro]
        .groupby("ClienteNome")["Vendas"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"Vendas": "Total Vendas ($)"})
    )
    resultado.index = range(1, len(resultado) + 1)
    return resultado


def q6_vendedores_mais_descontos_eua(df: pd.DataFrame) -> pd.DataFrame:
    """Q6 — Vendedores que mais concedem descontos nos EUA."""
    eua = df[df["ClientePaís"] == "USA"]
    resultado = (
        eua.groupby("VendedorNome")["Desconto"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"VendedorNome": "Vendedor", "Desconto": "Total Desconto ($)"})
    )
    resultado.index = range(1, len(resultado) + 1)
    return resultado


def q7_fornecedores_margem_vestuario_feminino(df: pd.DataFrame) -> pd.DataFrame:
    """Q7 — Fornecedores com maior margem de lucro em 'Womens wear'."""
    filtro = df["CategoriaNome"].str.strip() == "Womens wear"
    resultado = (
        df[filtro]
        .groupby("FornecedorNome")["Margem Bruta"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"FornecedorNome": "Fornecedor", "Margem Bruta": "Margem de Lucro ($)"})
    )
    resultado.index = range(1, len(resultado) + 1)
    return resultado


def q8_vendas_anuais_2009_2012(df: pd.DataFrame) -> tuple:
    """Q8 — Total vendido em 2009 e evolução anual de 2009 a 2012."""
    periodo = df[df["Ano"].between(2009, 2012)]
    vendas_ano = (
        periodo.groupby("Ano")["Vendas"]
        .sum()
        .reset_index()
        .rename(columns={"Ano": "Ano", "Vendas": "Total Vendas ($)"})
    )
    vendas_ano["Ano"] = vendas_ano["Ano"].astype(int)

    total_2009 = vendas_ano.loc[vendas_ano["Ano"] == 2009, "Total Vendas ($)"].values
    total_2009 = total_2009[0] if len(total_2009) > 0 else 0.0

    # Tendência: compara primeiro e último ano disponíveis
    if len(vendas_ano) >= 2:
        delta = vendas_ano["Total Vendas ($)"].iloc[-1] - vendas_ano["Total Vendas ($)"].iloc[0]
        crescimentos = vendas_ano["Total Vendas ($)"].diff().dropna()
        if all(crescimentos > 0):
            tendencia = "📈 CRESCENDO em todos os anos"
        elif all(crescimentos < 0):
            tendencia = "📉 DECAINDO em todos os anos"
        elif abs(delta) / vendas_ano["Total Vendas ($)"].iloc[0] < 0.05:
            tendencia = "➡️  ESTÁVEL (variação < 5% no período)"
        elif delta > 0:
            tendencia = "📈 CRESCENDO no período (com algumas oscilações)"
        else:
            tendencia = "📉 DECAINDO no período (com algumas oscilações)"
    else:
        tendencia = "Dados insuficientes"

    vendas_ano.index = range(1, len(vendas_ano) + 1)
    return total_2009, vendas_ano, tendencia


def q9_clientes_calcados_masc_2013(df: pd.DataFrame) -> tuple:
    """Q9 — Principais clientes de 'Men´s Footwear' em 2013 e cidades atendidas."""
    filtro = (
        (df["CategoriaNome"].str.strip() == "Men´s Footwear") &
        (df["Ano"] == 2013)
    )
    sub = df[filtro]

    clientes = (
        sub.groupby("ClienteNome")["Vendas"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"Vendas": "Total Vendas ($)"})
    )
    clientes.index = range(1, len(clientes) + 1)

    cidades = (
        sub.groupby("ClienteCidade")["Vendas"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"ClienteCidade": "Cidade", "Vendas": "Total Vendas ($)"})
    )
    cidades.index = range(1, len(cidades) + 1)

    return clientes, cidades


def q10_vendas_europa_por_pais(df: pd.DataFrame) -> pd.DataFrame:
    """Q10 — Vendas ($) por país na Europa."""
    europa = df[df["ClientePaís"].isin(PAISES_EUROPA)]
    resultado = (
        europa.groupby("ClientePaís")["Vendas"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"ClientePaís": "País", "Vendas": "Total Vendas ($)"})
    )
    resultado.index = range(1, len(resultado) + 1)
    return resultado


# ─────────────────────────────────────────────
#  FORMATAÇÃO / IMPRESSÃO
# ─────────────────────────────────────────────

def separador(titulo: str):
    print("\n" + "=" * 60)
    print(f"  {titulo}")
    print("=" * 60)

def formatar_df(df: pd.DataFrame) -> str:
    """Formata DataFrame com colunas numéricas em R$ / $ legível."""
    df_fmt = df.copy()
    for col in df_fmt.select_dtypes(include="number").columns:
        df_fmt[col] = df_fmt[col].apply(lambda x: f"$ {x:,.2f}")
    return df_fmt.to_string()


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    print("\n🔄  Carregando dados...")
    df = carregar_dados(CAMINHO_BASE)
    print(f"✅  {len(df):,} registros carregados.\n")

    pd.set_option("display.float_format", "{:,.2f}".format)

    # ── Q1 ──────────────────────────────────────
    separador("Q1 — Top 10 Clientes por Vendas ($)")
    print(formatar_df(q1_top10_clientes(df)))

    # ── Q2 ──────────────────────────────────────
    separador("Q2 — Top 3 Países por Vendas ($)")
    print(formatar_df(q2_top3_paises(df)))

    # ── Q3 ──────────────────────────────────────
    separador("Q3 — Categorias com Maior Faturamento no Brasil")
    print(formatar_df(q3_categorias_brasil(df)))

    # ── Q4 ──────────────────────────────────────
    separador("Q4 — Despesa de Frete por Transportadora")
    print(formatar_df(q4_frete_por_transportadora(df)))

    # ── Q5 ──────────────────────────────────────
    separador("Q5 — Clientes de 'Men´s Footwear' na Alemanha")
    r5 = q5_clientes_calcados_masc_alemanha(df)
    if r5.empty:
        print("  ⚠️  Nenhum registro encontrado para esse filtro.")
    else:
        print(formatar_df(r5))

    # ── Q6 ──────────────────────────────────────
    separador("Q6 — Vendedores com Mais Descontos nos EUA")
    print(formatar_df(q6_vendedores_mais_descontos_eua(df)))

    # ── Q7 ──────────────────────────────────────
    separador("Q7 — Fornecedores com Maior Margem em 'Womens wear'")
    print(formatar_df(q7_fornecedores_margem_vestuario_feminino(df)))

    # ── Q8 ──────────────────────────────────────
    separador("Q8 — Vendas em 2009 e Evolução Anual (2009–2012)")
    total_2009, vendas_anuais, tendencia = q8_vendas_anuais_2009_2012(df)
    print(f"\n  Total vendido em 2009: $ {total_2009:,.2f}\n")
    # Ano não deve ser formatado como moeda — imprime separado
    df_ano = vendas_anuais.copy()
    df_ano["Total Vendas ($)"] = df_ano["Total Vendas ($)"].apply(lambda x: f"$ {x:,.2f}")
    print(df_ano.to_string())
    print(f"\n  Conclusão da tendência: {tendencia}")

    # ── Q9 ──────────────────────────────────────
    separador("Q9 — Clientes e Cidades de 'Men´s Footwear' em 2013")
    clientes_2013, cidades_2013 = q9_clientes_calcados_masc_2013(df)
    if clientes_2013.empty:
        print("  ⚠️  Nenhum registro de 'Men´s Footwear' encontrado em 2013.")
        print("  (O dataset cobre até 2012 — dados de 2013 não estão presentes.)")
    else:
        print("\n  🔹 Por Cliente:")
        print(formatar_df(clientes_2013))
        print("\n  🔹 Por Cidade:")
        print(formatar_df(cidades_2013))

    # ── Q10 ─────────────────────────────────────
    separador("Q10 — Vendas ($) por País na Europa")
    print(formatar_df(q10_vendas_europa_por_pais(df)))

    print("\n" + "=" * 60)
    print("  ✅  Análise concluída!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()