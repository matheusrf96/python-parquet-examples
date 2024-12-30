
import csv
from typing import Any, Dict

import pandas as pd
from fastparquet import write


def retrieve_data_from_csv() -> Dict:
    data = {
        "prefixo_cnpj": [],
        "indicador_cnpj": [],
        "sufixo_cnpj": [],
        "indicador_de_matriz_filial": [],
        "razao_social": [],
        "situacao_cadastral": [],
        "data_situacao_cadastral": [],
        "motivo_situacao_cadastral": [],
        "nome_cidade_exterior": [],
        "codigo_pais": [],
        "data_inicio_atividade": [],
        "cnae_fiscal_principal": [],
        "cnae_fiscal_secundaria": [],
        "tipo_logradouro": [],
        "logradouro": [],
        "numero": [],
        "complemento": [],
        "bairro": [],
        "cep": [],
        "uf": [],
        "codigo_municipio": [],
        "ddd_1": [],
        "telefone_1": [],
        "ddd_2": [],
        "telefone_2": [],
        "ddd_fax": [],
        "telefone_fax": [],
        "email": [],
        "situacao_especial_da_empresa": [],
        "data_da_situacao_especial": [],
    }

    print("reading csv lines")
    FILE_PATH = "../files/estabelecimentos/Estabelecimentos1.csv"
    with open(FILE_PATH, newline="", encoding="latin-1") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=";", quotechar="\"")
        for row in spamreader:
            data["prefixo_cnpj"].append(row[0])
            data["indicador_cnpj"].append(row[1])
            data["sufixo_cnpj"].append(row[2])
            data["indicador_de_matriz_filial"].append(row[3])
            data["razao_social"].append(row[4])
            data["situacao_cadastral"].append(row[5])
            data["data_situacao_cadastral"].append(row[6])
            data["motivo_situacao_cadastral"].append(row[7])
            data["nome_cidade_exterior"].append(row[8])
            data["codigo_pais"].append(row[9])
            data["data_inicio_atividade"].append(row[10])
            data["cnae_fiscal_principal"].append(row[11])
            data["cnae_fiscal_secundaria"].append(row[12])
            data["tipo_logradouro"].append(row[13])
            data["logradouro"].append(row[14])
            data["numero"].append(row[15])
            data["complemento"].append(row[16])
            data["bairro"].append(row[17])
            data["cep"].append(row[18])
            data["uf"].append(row[19])
            data["codigo_municipio"].append(row[20])
            data["ddd_1"].append(row[21])
            data["telefone_1"].append(row[22])
            data["ddd_2"].append(row[23])
            data["telefone_2"].append(row[24])
            data["ddd_fax"].append(row[25])
            data["telefone_fax"].append(row[26])
            data["email"].append(row[27])
            data["situacao_especial_da_empresa"].append(row[28])
            data["data_da_situacao_especial"].append(row[29])

    return data


def convert_csv_data_into_pandas_dataframe(data: Dict) -> Any:
    print("converting csv data into pandas dataframe")
    return pd.DataFrame(data=data)


def convert_dataframe_into_parquet_file(df: Any) -> None:
    FILE_PATH = "../files/estabelecimentos/Estabelecimentos1.parq"
    print("creating parquet file")
    write(
        filename=FILE_PATH,
        data=df,
        row_group_offsets=[0, 10000, 20000],
        compression="GZIP",
        file_scheme="hive",
    )
    print("parquet file has been created successfully")


csv_data = retrieve_data_from_csv()
dataframe = convert_csv_data_into_pandas_dataframe(data=csv_data)
convert_dataframe_into_parquet_file(df=dataframe)
