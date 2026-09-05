"""
Baixa uma amostra do dataset de Yellow Taxi da NYC TLC (via NYC Open Data, que
disponibiliza os mesmos dados da TLC exportados em CSV) e salva em data/raw/.

A TLC hoje so disponibiliza os arquivos oficiais em Parquet, entao usamos a
mesma base exportada em CSV pelo NYC Open Data (https://data.cityofnewyork.us).

Rodar antes de abrir o notebook, caso prefira nao depender de internet durante
a execucao dele (o notebook faz esse mesmo download sozinho se o arquivo nao
existir, mas pode rodar esse script manualmente antes se preferir separar as
etapas):

    python scripts/baixar_dataset.py
"""
import argparse
import os
import urllib.parse
import urllib.request

DATASET_BASE_URL = "https://data.cityofnewyork.us/resource/4b4i-vvec.csv"  # NYC Open Data - Yellow Taxi 2023
DATASET_PARAMS = {
    "$limit": "5000",
    "$where": "tpep_pickup_datetime between '2023-01-01T00:00:00' and '2023-01-07T23:59:59'",
    "$order": "tpep_pickup_datetime",
}
DESTINO_PADRAO = os.path.join("data", "raw", "yellow_tripdata.csv")


def baixar_dataset(destino, base_url=DATASET_BASE_URL, params=None):
    url = base_url + "?" + urllib.parse.urlencode(params or DATASET_PARAMS)
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    print(f"baixando dataset de {url}")
    urllib.request.urlretrieve(url, destino)
    print(f"salvo em {destino}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destino", default=DESTINO_PADRAO, help="Caminho de destino do csv")
    args = parser.parse_args()
    baixar_dataset(args.destino)
