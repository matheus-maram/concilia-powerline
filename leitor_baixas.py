from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def _read_file_content(file_source: Any) -> str:
    if isinstance(file_source, (str, Path)):
        with open(file_source, "r", encoding="latin1") as handle:
            return handle.read()

    if hasattr(file_source, "getvalue"):
        data = file_source.getvalue()
    else:
        if hasattr(file_source, "seek"):
            file_source.seek(0)
        data = file_source.read()

    if isinstance(data, bytes):
        return data.decode("latin1")

    if isinstance(data, str):
        return data

    raise TypeError("Unsupported file_source type for processar_baixas")


def processar_baixas(file_source) -> pd.DataFrame:
    linhas = _read_file_content(file_source).splitlines()

    registros = []
    centro_atual = None

    for linha in linhas[5:]:
        linha = linha.strip()
        if not linha:
            continue

        partes = [parte.strip() for parte in linha.split(";")]
        if not partes:
            continue

        primeira_coluna = partes[0]

        if (
            primeira_coluna == ""
            or primeira_coluna.startswith("Data")
            or primeira_coluna.startswith("Subtotal")
            or primeira_coluna.startswith("Sistema Posto Delta")
            or primeira_coluna.startswith("Total")
        ):
            continue

        if primeira_coluna and all(valor == "" for valor in partes[1:]):
            centro_atual = primeira_coluna
            continue

        if "/" not in primeira_coluna:
            continue

        data = partes[0]
        lancamento = partes[1] if len(partes) > 1 else None
        conta = partes[2] if len(partes) > 2 else None

        documento = None
        valor_total = None
        data_baixa = None
        lancamento_baixa = None
        responsavel = None

        if len(partes) > 10 and partes[10]:
            lancamento_baixa = partes[10]
            responsavel = partes[3] if len(partes) > 3 else None
            documento = partes[5] if len(partes) > 5 else None
            valor_total = partes[6] if len(partes) > 6 else None
            data_baixa = partes[8] if len(partes) > 8 else None
        elif len(partes) > 11 and partes[11]:
            lancamento_baixa = partes[11]
            responsavel = partes[4] if len(partes) > 4 else None
            documento = partes[6] if len(partes) > 6 else None
            valor_total = partes[7] if len(partes) > 7 else None
            data_baixa = partes[9] if len(partes) > 9 else None
        elif len(partes) > 13 and partes[13]:
            lancamento_baixa = partes[13]
            responsavel = partes[3] if len(partes) > 3 else None
            documento = partes[6] if len(partes) > 6 else None
            valor_total = partes[7] if len(partes) > 7 else None
            data_baixa = partes[9] if len(partes) > 9 else None

        registros.append(
            {
                "Centro de Resultados": centro_atual,
                "Data": data,
                "Lancamento": lancamento,
                "Conta": conta,
                "Responsável": responsavel,
                "Documento": documento,
                "Valor Total": valor_total,
                "Data Baixa": data_baixa,
                "Lancamento Baixa": lancamento_baixa,
            }
        )

    df_final = pd.DataFrame(
        registros,
        columns=[
            "Centro de Resultados",
            "Data",
            "Lancamento",
            "Conta",
            "Responsável",
            "Documento",
            "Valor Total",
            "Data Baixa",
            "Lancamento Baixa",
        ],
    )

    if df_final.empty:
        return df_final

    df_final["Data"] = pd.to_datetime(df_final["Data"], format="%d/%m/%Y", errors="coerce")
    df_final["Data Baixa"] = pd.to_datetime(
        df_final["Data Baixa"], format="%d/%m/%Y", errors="coerce"
    )
    df_final["Valor Total"] = (
        df_final["Valor Total"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df_final["Valor Total"] = pd.to_numeric(df_final["Valor Total"], errors="coerce")

    return df_final
