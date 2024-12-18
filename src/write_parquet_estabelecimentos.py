
import csv
from typing import Any, Dict

import pandas as pd
from fastparquet import write


def retrieve_data_from_csv() -> Dict:
    data = {
        "cnpj_1": [],
        "cnpj_2": [],
        "cnpj_3": [],
        "identificador_matriz_filial": [],
        "nome_fantasia": [],
        "situacao_cadastral": [],
        "data_situacao_cadastral": [],
        "motivo_situacao_cadastral": [],
        "nome_cidade_exterior": [],
        "nome_pais": [],
        "codigo_1": [],
        "codigo_2": [],
        "codigo_3": [],
        "tipo_rua": [],
        "rua": [],
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
        "correio_eletronico": [],
        "qualificacao_do_responsavel": [],
        "capital_social": [],
    }

    print("reading csv lines")
    FILE_PATH = "../files/estabelecimentos/Estabelecimentos1.csv"
    with open(FILE_PATH, newline="", encoding="latin-1") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=";", quotechar="\"")
        for row in spamreader:
            data["cnpj_1"].append(row[0])
            data["cnpj_2"].append(row[1])
            data["cnpj_3"].append(row[2])
            data["identificador_matriz_filial"].append(row[3])
            data["nome_fantasia"].append(row[4])
            data["situacao_cadastral"].append(row[5])
            data["data_situacao_cadastral"].append(row[6])
            data["motivo_situacao_cadastral"].append(row[7])
            data["nome_cidade_exterior"].append(row[8])
            data["nome_pais"].append(row[9])
            data["codigo_1"].append(row[10])
            data["codigo_2"].append(row[11])
            data["codigo_3"].append(row[12])
            data["tipo_rua"].append(row[13])
            data["rua"].append(row[14])
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
            data["correio_eletronico"].append(row[27])
            data["qualificacao_do_responsavel"].append(row[28])
            data["capital_social"].append(row[29])

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
