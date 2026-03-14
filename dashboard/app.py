import os
import streamlit as st
import pandas as pd
import snowflake.connector

# =====================================
# CONFIGURAÇÃO DA PÁGINA
# =====================================
st.set_page_config(
    page_title="UrbanFlow Mobility Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =====================================
# CONFIGURAÇÕES SNOWFLAKE
# =====================================
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER", "LEANDROSLAXDE"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT", "ctc05330.us-east-1"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "URBANFLOW_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "URBANFLOW"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "DBT_DEV"),
}

QUERY = """
SELECT
    DATA,
    TOTAL_VIAGENS,
    TOTAL_REGISTROS,
    TOTAL_PASSAGEIROS,
    TOTAL_MOTORISTAS,
    FATURAMENTO,
    TICKET_MEDIO,
    TEMPERATURA_MEDIA,
    VELOCIDADE_MEDIA_TRAFEGO,
    NIVEL_CONGESTIONAMENTO_MEDIO,
    STATUS_CLIMA,
    STATUS_TRAFEGO
FROM URBANFLOW.DBT_DEV.MART_OPERACAO_DIARIA
ORDER BY DATA
"""

# =====================================
# VALIDAÇÃO CONFIG
# =====================================
def validate_config():
    missing = []

    if not SNOWFLAKE_CONFIG.get("user"):
        missing.append("SNOWFLAKE_USER")
    if not SNOWFLAKE_CONFIG.get("password"):
        missing.append("SNOWFLAKE_PASSWORD")
    if not SNOWFLAKE_CONFIG.get("account"):
        missing.append("SNOWFLAKE_ACCOUNT")
    if not SNOWFLAKE_CONFIG.get("warehouse"):
        missing.append("SNOWFLAKE_WAREHOUSE")

    if missing:
        st.error("Variáveis de ambiente faltando: " + ", ".join(missing))
        st.stop()

# =====================================
# CARREGAMENTO DE DADOS
# =====================================
@st.cache_data(show_spinner=False)
def load_data():

    validate_config()

    conn = None
    cur = None

    try:

        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)

        cur = conn.cursor()

        cur.execute(QUERY)

        rows = cur.fetchall()

        cols = [desc[0] for desc in cur.description]

        df = pd.DataFrame(rows, columns=cols)

    except Exception as e:

        st.error(f"Erro ao conectar no Snowflake: {e}")
        st.stop()

    finally:

        if cur:
            cur.close()

        if conn:
            conn.close()

    if not df.empty:

        df.columns = [c.upper() for c in df.columns]

        df["DATA"] = pd.to_datetime(df["DATA"])

        numeric_cols = [
            "TOTAL_VIAGENS",
            "TOTAL_REGISTROS",
            "TOTAL_PASSAGEIROS",
            "TOTAL_MOTORISTAS",
            "FATURAMENTO",
            "TICKET_MEDIO",
            "TEMPERATURA_MEDIA",
            "VELOCIDADE_MEDIA_TRAFEGO",
            "NIVEL_CONGESTIONAMENTO_MEDIO",
        ]

        for col in numeric_cols:

            if col in df.columns:

                df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

# =====================================
# FORMATADORES
# =====================================
def br_number(value, decimals=0):

    if pd.isna(value):
        return "-"

    if decimals == 0:
        return f"{int(round(value)):,}".replace(",", ".")

    return (
        f"{value:,.{decimals}f}"
        .replace(",", "X")
        .replace(".", ",")
        .replace("X", ".")
    )


def br_currency(value):

    if pd.isna(value):
        return "-"

    return (
        f"R$ {value:,.2f}"
        .replace(",", "X")
        .replace(".", ",")
        .replace("X", ".")
    )

# =====================================
# APP
# =====================================
st.title("UrbanFlow Mobility Dashboard")

st.caption(
    "Painel analítico de mobilidade urbana com dados de viagens, clima e tráfego."
)

df = load_data()

if df.empty:
    st.warning("Tabela MART_OPERACAO_DIARIA sem dados.")
    st.stop()

# =====================================
# SIDEBAR
# =====================================
st.sidebar.header("Filtros")

min_date = df["DATA"].min().date()
max_date = df["DATA"].max().date()

periodo = st.sidebar.date_input(
    "Período",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

if isinstance(periodo, tuple):

    data_inicio, data_fim = periodo

else:

    data_inicio = min_date
    data_fim = max_date

df_filtrado = df[
    (df["DATA"].dt.date >= data_inicio)
    & (df["DATA"].dt.date <= data_fim)
]

# =====================================
# KPIs
# =====================================
total_viagens = df_filtrado["TOTAL_VIAGENS"].sum()
faturamento_total = df_filtrado["FATURAMENTO"].sum()
ticket_medio = df_filtrado["TICKET_MEDIO"].mean()
temperatura_media = df_filtrado["TEMPERATURA_MEDIA"].mean()

c1, c2, c3, c4 = st.columns(4)

c1.metric("Total de viagens", br_number(total_viagens))

c2.metric("Faturamento total", br_currency(faturamento_total))

c3.metric("Ticket médio", br_currency(ticket_medio))

c4.metric(
    "Temperatura média",
    f"{br_number(temperatura_media,2)} °C"
)

# =====================================
# GRÁFICOS
# =====================================
st.markdown("### Evolução operacional")

g1, g2 = st.columns(2)

with g1:

    st.subheader("Viagens por dia")

    serie = df_filtrado.set_index("DATA")["TOTAL_VIAGENS"]

    st.line_chart(serie)

with g2:

    st.subheader("Faturamento por dia")

    serie = df_filtrado.set_index("DATA")["FATURAMENTO"]

    st.line_chart(serie)

# =====================================
# CLIMA
# =====================================
st.markdown("### Condições externas")

g3, g4 = st.columns(2)

with g3:

    st.subheader("Temperatura x viagens")

    scatter_df = df_filtrado[
        ["TEMPERATURA_MEDIA", "TOTAL_VIAGENS"]
    ].dropna()

    if len(scatter_df) > 1:

        st.scatter_chart(
            scatter_df,
            x="TEMPERATURA_MEDIA",
            y="TOTAL_VIAGENS",
        )

    else:

        st.info("Poucos dados climáticos")

with g4:

    st.subheader("Congestionamento médio")

    trafego_df = df_filtrado[
        ["DATA", "NIVEL_CONGESTIONAMENTO_MEDIO"]
    ].dropna()

    if not trafego_df.empty:

        serie = trafego_df.set_index("DATA")[
            "NIVEL_CONGESTIONAMENTO_MEDIO"
        ]

        st.line_chart(serie)

    else:

        st.info("Sem dados de tráfego")

# =====================================
# VELOCIDADE TRÁFEGO
# =====================================
st.markdown("### Tráfego")

vel_df = df_filtrado[
    ["DATA", "VELOCIDADE_MEDIA_TRAFEGO"]
].dropna()

if not vel_df.empty:

    serie = vel_df.set_index("DATA")[
        "VELOCIDADE_MEDIA_TRAFEGO"
    ]

    st.line_chart(serie)

else:

    st.info("Sem dados de velocidade média")

# =====================================
# TABELA
# =====================================
st.markdown("### Tabela analítica")

df_view = df_filtrado.copy()

df_view["DATA"] = df_view["DATA"].dt.date

st.dataframe(
    df_view,
    use_container_width=True,
    hide_index=True,
)