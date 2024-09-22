from pyspark.sql import SparkSession
import json
import boto3
from botocore.client import Config
from botocore.exceptions import NoCredentialsError
from io import BytesIO
import time
import requests
from pyspark.sql.functions import udf
from http.cookiejar import LWPCookieJar
import datetime
from pyspark.sql.functions import col, udf


def upload_json_to_minio(data, bucket, object_name, path_save):
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='datalake',
        aws_secret_access_key='datalake',
    )

    try:
        json_bytes = json.dumps(data).encode('utf-8')

        json_buffer = BytesIO(json_bytes)

        s3_client.upload_fileobj(
            json_buffer,
            bucket,
            path_save
         )
        print(f'Arquivo JSON foi carregado com sucesso no bucket {bucket}, objeto: {object_name}')
        time.sleep(0.4)
    except NoCredentialsError:
        print("Credenciais não encontradas")



def fetch_data(id_linha):
  post_auth = "http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token=d3f026bb74699c88e75ef5a7e71cf1181e70cd26cb93b8b31038c9df15ba2f61"

  session = requests.Session()
  session.post(post_auth)

  url = "http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token=d3f026bb74699c88e75ef5a7e71cf1181e70cd26cb93b8b31038c9df15ba2f61"
  session = requests.Session()
  response = session.post(url)

  if response.status_code == 200:
      cookies = response.cookies

      endpoint = "http://api.olhovivo.sptrans.com.br/v2.1/Previsao/Linha?"
      params = {
            "codigoLinha": id_linha,
        }
      response = session.get(endpoint, params=params, cookies=cookies)

      time = datetime.datetime.now()
      bucket = "raw"
      name_file = f"previsao_chegada_{time}.json"
      caminho_de_salvar = f"/previsao_chegada/{name_file}"

      upload_json_to_minio(
          data=response.json(),
          bucket=bucket,
          object_name=name_file,
          path_save=caminho_de_salvar
      )

      return True  # assuming API returns a list of records
  else:
      return False

def main():

  spark = SparkSession.builder \
    .appName('User Raw to Trusted') \
    .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()

  trusted_path_posicao_veiculo = 's3a://trusted/posicao_veiculo'
  df_posicao_to_previsao = spark.read.format(
      'delta'
      ).load(trusted_path_posicao_veiculo)


  convertUDF = udf(lambda id: fetch_data(id))

  df_previsao_responde2 = df_posicao_to_previsao.select("ID_LINHA").distinct()
  df_previsao_responde2.withColumn("response",convertUDF(col("id_linha")))

  return True


if __name__ == "__main__":
    main()
