# Trabalho Stream Processing Pipelines

Esse repositório é o trabalho final da disciplina de Stream Processing Pipelines. 

Integrantes:
* Guilherme Csorgo Henriques: 370073
* Karen Luzia Vitório Martins: 370096
* Ludmila Rocha Silva: 372484
* Thiago Guilherme: 375344

## Escolhas para o trabalho

* **Plataforma:** Databricks
* **Ferramenta:** Spark Streaming
* **Dataset:** [NYC Yellow Taxi Trip Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
* **Formato de entrada:** csv
* **Formato de saída:** parquet

Usamos bastante o **databricks** nas aulas, e por já ser de conhecimento nosso (e ser de fácil adoção), entendemos que seria um bom ponto de partida pensando em tempo e esforço para implementar o trabalho.

Optamos pelo **Spark Streaming** em vez do Flink porque a curva de aprendizado é bem menor pra quem está começando com streaming, e a API em Python (PySpark) já é bem próxima do que usamos no dia a dia com DataFrames. Optamos por essa ferramenta também por podermos trabalhar com **microbatch**, o que acaba sendo a realidade mais próxima do que trabalhamos hoje. Flink tem vantagens em latência e é mais "streaming nativo", mas entendemos fazer menos sentido para esse trabalho.

Como dataset, escolhemos o de **corridas de táxi de Nova York**, disponibilizado pela própria NYC TLC. A ideia é simular a chegada desses dados aos poucos, como se fossem corridas acontecendo em tempo real, em vez de carregar tudo de uma vez. Gostamos desse dataset porque ele tem campos pra trabalhar com tempo (horário de embarque), categorias (zona, forma de pagamento) e valores numéricos (tarifa, distância), o que dá material suficiente pra fazer filtro, agregação e janela de tempo sem precisar inventar dados.

A ingestão vai ler os **dados em csv**, simulando arquivos chegando numa pasta conforme o pipeline roda. A saída vai ser gravada em **Parquet**, que é o formato que o próprio enunciado sugere como exemplo de transformação, além de ser mais eficiente pra leitura posterior do que o csv ou json.

## Como rodar

O pipeline está no `stream_pipeline_nyc_taxi.ipynb`. É só importar no Databricks e rodar todas as células, ele mesmo detecta que está lá e ajusta os caminhos pra usar o disco local do cluster (`/tmp`), evitando o DBFS, que em vários workspaces novos (principalmente o Free Edition) vem com o root público desabilitado. Se o dataset real não estiver em `data/raw`, o notebook baixa uma amostra automaticamente (ver seção "Como funciona?" abaixo).

Local (fora do Databricks) também funciona, com `pip install -r requirements.txt`, mas só testamos rodando em Linux/Mac/WSL. No Windows puro o Spark local esbarra num problema conhecido do Hadoop (precisa do `winutils.exe` e da variável `HADOOP_HOME` configurados), então recomendamos rodar via Databricks mesmo, que é a plataforma que escolhemos pra entrega.

## Como funciona?

Passo a passo do que o notebook faz, na ordem que ele roda:

1. **Setup**: confere se o pyspark instalado é a versão 3.5.x (compatível com Java 8/11/17). Se estiver com a 4.x, ou sem pyspark nenhum, reinstala a versão certa sozinho. No Databricks essa etapa nem roda, o cluster já vem pronto.
2. **Ambiente**: detecta se está rodando no Databricks ou local e ajusta os caminhos. No Databricks usa o disco local do cluster (`/tmp`) em vez do DBFS, porque vários workspaces novos (principalmente o Free Edition) vêm com o DBFS root público desabilitado.
3. **Sessão Spark**: cria a sessão (ou reaproveita a que o Databricks já deixa pronta).
4. **Download dos dados**: baixa uma amostra da base do NYC TLC (primeira semana de janeiro/2023, aprox. 5000 corridas) via NYC Open Data, e salva em `data/raw/yellow_tripdata.csv`. Só baixa se o arquivo ainda não existir. **Se o download falhar aqui (por exemplo, sem internet no ambiente), rode `python scripts/baixar_dataset.py` manualmente antes de tentar de novo**, ou baixe pela URL que aparece na mensagem de erro.
5. **Schema**: define as colunas e tipos esperados do csv na mão, porque o Spark exige um schema fixo em streaming (não dá pra inferir sozinho).
6. **Simulação do stream**: função que parte o csv baixado em pedaços de 200 linhas e vai escrevendo na pasta `data/stream_source`, um arquivo a cada 2 segundos, imitando corridas chegando aos poucos em vez de tudo de uma vez.
7. **Ingestão e validação (bônus 1)**: o `readStream` lê da pasta de origem e descarta corrida sem passageiro, sem distância, com tarifa zerada ou negativa, e linha com campo essencial nulo. O dataset real já vem com colunas nulas (cerca de 324 das 5000 linhas nos nossos testes), então vai dar pra ver uma diverença de verdade entre a entrada e a saída de dados.
8. **Output em Parquet**: grava o resultado validado em `output/parquet`, com checkpoint em `output/checkpoints`.
9. **Rodando a simulação**: dispara a função do passo 6, que alimenta a pasta enquanto o stream (já iniciado no passo anterior) vai consumindo e processando em paralelo.
10. **Resultado esperado**: lê de volta o parquet gravado e mostra o total de linhas e uma amostra dos dados. Com os parâmetros padrão (10 arquivos de 200 linhas), o esperado é um número de linhas um pouco menor que 2000, por causa do filtro do passo 7, e os arquivos parquet aparecendo dentro de `output/parquet`.
