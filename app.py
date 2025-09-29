# ============================================
# Imports e configuração inicial
# ============================================
import streamlit as st
import pandas as pd
import io
from datetime import date
from thefuzz import fuzz
from leitor_extrato_santander import ler_extrato_santander_xlsx
from leitor_baixas import processar_baixas


# ============================================
# Utilitários
# ============================================
def make_safe_sheet_name(name: str) -> str:
    """Sanitiza o nome da planilha do Excel removendo caracteres inválidos"""
    invalid = set('[]:*?/\\')
    cleaned_chars = []
    for ch in str(name):
        code = ord(ch)
        if ch in invalid:
            continue
        if 32 <= code <= 126:  # apenas ASCII imprimível
            cleaned_chars.append(ch)
    cleaned = ''.join(cleaned_chars).strip()
    if not cleaned:
        cleaned = 'Sheet1'
    return cleaned[:31]


def format_currency_br(v):
    """Formata número como moeda brasileira"""
    if pd.isna(v):
        return ""
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)


def format_date_excel(df, cols):
    """Formata colunas de datas no padrão dd/mm/yyyy apenas para exportação"""
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.strftime("%d/%m/%Y")
    return df


# ============================================
# Conciliação em 3 níveis
# ============================================
def conciliar_multi_nivel(
    df_extrato: pd.DataFrame,
    df_baixas: pd.DataFrame,
    tolerancia_dias: int = 3,
    limite_similaridade: int = 85
) -> pd.DataFrame:
    """
    Nível 1: Valor idêntico
    Nível 2: Valor + Data próxima (± tolerancia_dias)
    Nível 3: Valor + Similaridade de nomes
    """

    ext = df_extrato.copy()
    bx = df_baixas.copy()

    # IDs únicos
    ext["Id Extrato"] = range(1, len(ext) + 1)
    bx["Id Baixa"] = range(1, len(bx) + 1)

    # Apenas saídas (negativos)
    ext = ext[ext["Valor"] < 0].copy()
    ext["Valor_Abs"] = ext["Valor"].abs()
    bx["Valor_Abs"] = bx["Valor Total"].abs()

    # Converter datas
    ext["Data"] = pd.to_datetime(ext["Data"], errors="coerce")
    bx["Data"] = pd.to_datetime(bx.get("Data"), errors="coerce")
    bx["Data Baixa"] = pd.to_datetime(bx["Data Baixa"], errors="coerce")

    # Flags
    ext["_conc"] = False
    bx["_conc"] = False

    matches = []  # (i_ext, i_bx, nivel, detalhe)

    # ---------- Nível 1: valor idêntico ----------
    for val in ext["Valor_Abs"].dropna().unique():
        ext_cand = ext[(ext["Valor_Abs"] == val) & (~ext["_conc"])]
        bx_cand = bx[(bx["Valor_Abs"] == val) & (~bx["_conc"])]
        if len(ext_cand) == 1 and len(bx_cand) == 1:
            i_e = ext_cand.index[0]
            i_b = bx_cand.index[0]
            ext.at[i_e, "_conc"] = True
            bx.at[i_b, "_conc"] = True
            matches.append((i_e, i_b, "Nível 1 (Valor)", "Valor idêntico"))

    # ---------- Nível 2: valor + data ----------
    for i_e, row_e in ext[~ext["_conc"]].iterrows():
        candidatos = bx[(bx["Valor_Abs"] == row_e["Valor_Abs"]) & (~bx["_conc"])]
        if candidatos.empty:
            continue
        melhor = None
        melhor_delta = None
        for i_b, row_b in candidatos.iterrows():
            if pd.isna(row_e["Data"]) or pd.isna(row_b["Data Baixa"]):
                continue
            delta = abs((row_e["Data"] - row_b["Data Baixa"]).days)
            if delta <= tolerancia_dias and (melhor is None or delta < melhor_delta):
                melhor, melhor_delta = i_b, delta
        if melhor is not None:
            ext.at[i_e, "_conc"] = True
            bx.at[melhor, "_conc"] = True
            matches.append((i_e, melhor, "Nível 2 (Valor+Data)", f"Δ {melhor_delta} dia(s)"))

    # ---------- Nível 3: valor + nome ----------
    for i_e, row_e in ext[~ext["_conc"]].iterrows():
        candidatos = bx[(bx["Valor_Abs"] == row_e["Valor_Abs"]) & (~bx["_conc"])]
        if candidatos.empty:
            continue
        nome_e = str(row_e["Responsável"] or "")
        melhor, melhor_score = None, -1
        for i_b, row_b in candidatos.iterrows():
            nome_b = str(row_b["Responsável"] or "")
            score = fuzz.token_sort_ratio(nome_e, nome_b)
            if score > melhor_score:
                melhor, melhor_score = i_b, score
        if melhor is not None and melhor_score >= limite_similaridade:
            ext.at[i_e, "_conc"] = True
            bx.at[melhor, "_conc"] = True
            matches.append((i_e, melhor, "Nível 3 (Valor+Nome)", f"similaridade {melhor_score}%"))

    # ---------- Construir resultado ----------
    linhas = []

    id_conc = 1
    for i_e, i_b, nivel, detalhe in matches:
        linhas.append({
            "Id Conciliado": id_conc,
            "Id Extrato": ext.at[i_e, "Id Extrato"],
            "Id Baixa": bx.at[i_b, "Id Baixa"],
            "Data Extrato": ext.at[i_e, "Data"],
            "Valor Extrato": ext.at[i_e, "Valor"],
            "Data Baixa": bx.at[i_b, "Data Baixa"],
            "Valor Baixa": bx.at[i_b, "Valor Total"],
            "Status": "✅ Conciliado",
            "Nível Conciliação": nivel,
            "Detalhe": detalhe
        })
        id_conc += 1

    # Só no extrato
    for i_e, row_e in ext[~ext["_conc"]].iterrows():
        linhas.append({
            "Id Conciliado": None,
            "Id Extrato": row_e["Id Extrato"],
            "Id Baixa": None,
            "Data Extrato": row_e["Data"],
            "Valor Extrato": row_e["Valor"],
            "Status": "❌ Só no Extrato"
        })

    # Só nas baixas
    for i_b, row_b in bx[~bx["_conc"]].iterrows():
        linhas.append({
            "Id Conciliado": None,
            "Id Extrato": None,
            "Id Baixa": row_b["Id Baixa"],
            "Data Baixa": row_b["Data Baixa"],
            "Valor Baixa": row_b["Valor Total"],
            "Status": "⚠️ Só nas Baixas"
        })

    return pd.DataFrame(linhas)


# ============================================
# Streamlit App
# ============================================
st.set_page_config(page_title="Conciliação Bancária", layout="wide")
st.title("🔎 Conciliação Bancária")
st.markdown("Conciliação em **3 níveis**: Valor → Valor + Data (±3 dias) → Valor + Similaridade de Nomes.")


col1, col2 = st.columns(2)
with col1:
    extrato_file = st.file_uploader("📂 Upload do Extrato Santander (.xlsx)", type=["xlsx"])
with col2:
    baixas_file = st.file_uploader("📂 Upload da Relação de Baixas (.csv)", type=["csv"])

if extrato_file and baixas_file:
    df_extrato = ler_extrato_santander_xlsx(extrato_file)
    df_baixas = processar_baixas(baixas_file)

    df_extrato_saidas = df_extrato[df_extrato["Valor"] < 0].copy()
    df_baixas["Valor_Abs"] = df_baixas["Valor Total"].abs()

    st.success("✅ Arquivos carregados com sucesso!")

    # Totais
    col_m1, col_m2 = st.columns(2)
    with col_m1:
        st.metric("📉 Saídas no Extrato", len(df_extrato_saidas))
        st.metric("💸 Total Saídas", format_currency_br(df_extrato_saidas["Valor"].sum()))
    with col_m2:
        st.metric("📋 Registros nas Baixas", len(df_baixas))
        st.metric("💰 Total Baixas", format_currency_br(df_baixas["Valor Total"].sum()))

    # Exportação limpa
    st.divider()
    st.subheader("💾 Exportar Arquivos Limpos")
    col_exp1, col_exp2 = st.columns(2)
    with col_exp1:
        buf_ext = io.BytesIO()
        with pd.ExcelWriter(buf_ext, engine="openpyxl") as wr:
            format_date_excel(df_extrato, ["Data"]).to_excel(wr, index=False, sheet_name="Extrato")
        buf_ext.seek(0)
        st.download_button("📥 Baixar Extrato Limpo", buf_ext.getvalue(), file_name="extrato_limpo.xlsx")
    with col_exp2:
        buf_bx = io.BytesIO()
        with pd.ExcelWriter(buf_bx, engine="openpyxl") as wr:
            format_date_excel(df_baixas, ["Data", "Data Baixa"]).to_excel(wr, index=False, sheet_name="Baixas")
        buf_bx.seek(0)
        st.download_button("📥 Baixar Baixas Limpas", buf_bx.getvalue(), file_name="baixas_limpas.xlsx")

    # Conciliação
    st.divider()
    if st.button("🔄 Processar Conciliação", type="primary", use_container_width=True):
        df_result = conciliar_multi_nivel(df_extrato, df_baixas)
        st.session_state["resultado_conciliacao"] = df_result

    # Resultado
    if "resultado_conciliacao" in st.session_state:
        res = st.session_state["resultado_conciliacao"].copy()

        st.subheader("📊 Resultado da Conciliação")
        conc = (res["Status"] == "✅ Conciliado").sum()
        so_ext = (res["Status"] == "❌ Só no Extrato").sum()
        so_bx = (res["Status"] == "⚠️ Só nas Baixas").sum()
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("✅ Conciliados", conc)
        with c2: st.metric("❌ Só no Extrato", so_ext)
        with c3: st.metric("⚠️ Só nas Baixas", so_bx)

        # Apenas exportação
        st.divider()
        buf_res = io.BytesIO()
        with pd.ExcelWriter(buf_res, engine="openpyxl") as wr:
            format_date_excel(res, ["Data Extrato", "Data Lançamento", "Data Baixa"]).to_excel(wr, index=False, sheet_name="Conciliado")
            format_date_excel(df_extrato, ["Data"]).to_excel(wr, index=False, sheet_name="Extrato")
            format_date_excel(df_baixas, ["Data", "Data Baixa"]).to_excel(wr, index=False, sheet_name="Baixas")
        buf_res.seek(0)
        st.download_button(
            "📥 Download Excel Completo",
            buf_res.getvalue(),
            file_name=f"conciliacao_completa_{date.today().strftime('%Y-%m-%d')}.xlsx"
        )
else:
    st.info("👆 Faça o upload dos dois arquivos para começar a análise.")
