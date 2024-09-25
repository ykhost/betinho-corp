from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, unix_timestamp, abs, desc, percentile_approx


jdbc_driver_path = "/util/jar/postgresql-42.5.1.jar"
url = "jdbc:postgresql://db:5432/SPTRANS"
properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

def main():
  trusted_path_previsao_chegada = 's3a://trusted/previsao_chegada'
  trusted_path_posicao_veiculo = 's3a://trusted/posicao_veiculo'

  spark = SparkSession.builder \
    .appName('GOLD_PREV_CHEGADA') \
    .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

  df_previsao_chegada = spark.read.format('delta').load(trusted_path_previsao_chegada)
  df_posicao_veiculo = spark.read.format('delta').load(trusted_path_posicao_veiculo).select("ID_LINHA", "LETREIRO_LINHA", "LETREIRO_DESTINO_LINHA", "LETREIRO_ORIGEM_LINHA", "ID_VEICULO").distinct()

  df_previsao_chegada_join = df_previsao_chegada.join(df_posicao_veiculo, col("ID_VEICULO") == col("PREFIXO_VEICULO"), "inner")
  df_previsao_chegada2 = df_previsao_chegada_join.withColumn("diff_in_minute", (abs(unix_timestamp("dat_ref_carga","HH:mm")-unix_timestamp("horario_previsto_cheada","HH:mm"))/60))

  df_insight_velocidade = df_previsao_chegada2.withColumn("velocidade_para_chegar", (col("distancia_km") * 60) / col("diff_in_minute"))

  df_velocidade_maior_40 = df_insight_velocidade.filter("velocidade_para_chegar > 40")

  df_velocidade_menor_10 = df_insight_velocidade.filter("velocidade_para_chegar < 10")

  df_velocidade_maior_40_parada = df_velocidade_maior_40.groupBy("ID_PARADA","nome_parada").agg(
    percentile_approx("velocidade_para_chegar", 0.5).alias("media_velocidade_chegada"),
    percentile_approx("diff_in_minute", 0.5).alias("MEDIA_DIFF_PREV_CHEGADA_MINUTOS"),
    percentile_approx("distancia_km", 0.5).alias("media_distancia_km")
  ).sort(desc("media_velocidade_chegada")).limit(20)

  df_velocidade_maior_40_LINHA = df_velocidade_maior_40.groupBy(
     "LETREIRO_LINHA",
     "LETREIRO_DESTINO_LINHA",
     "LETREIRO_ORIGEM_LINHA"
    ).agg(
      percentile_approx("velocidade_para_chegar", 0.5).alias("media_velocidade_chegada"),
      percentile_approx("diff_in_minute", 0.5).alias("MEDIA_DIFF_PREV_CHEGADA_MINUTOS"),
      percentile_approx("distancia_km", 0.5).alias("media_distancia_km")
  ).sort(desc("media_velocidade_chegada"))

  df_velocidade_maior_40_VEICULO = df_velocidade_maior_40.groupBy("ID_VEICULO").agg(
      percentile_approx("velocidade_para_chegar", 0.5).alias("media_velocidade_chegada"),
      percentile_approx("diff_in_minute", 0.5).alias("MEDIA_DIFF_PREV_CHEGADA_MINUTOS"),
      percentile_approx("distancia_km", 0.5).alias("media_distancia_km")
  ).sort(desc("media_velocidade_chegada"))


  df_velocidade_menor_10_parada = df_velocidade_menor_10.groupBy("ID_PARADA","nome_parada").agg(
      percentile_approx("velocidade_para_chegar", 0.5).alias("media_velocidade_chegada"),
      percentile_approx("diff_in_minute", 0.5).alias("MEDIA_DIFF_PREV_CHEGADA_MINUTOS"),
      percentile_approx("distancia_km", 0.5).alias("media_distancia_km")
  ).sort(desc("MEDIA_DIFF_PREV_CHEGADA_MINUTOS"),"media_velocidade_chegada")

  df_velocidade_menor_10_linha = df_velocidade_menor_10.groupBy("LETREIRO_LINHA","LETREIRO_DESTINO_LINHA","LETREIRO_ORIGEM_LINHA").agg(
      percentile_approx("velocidade_para_chegar", 0.5).alias("media_velocidade_chegada"),
      percentile_approx("diff_in_minute", 0.5).alias("MEDIA_DIFF_PREV_CHEGADA_MINUTOS"),
      percentile_approx("distancia_km", 0.5).alias("media_distancia_km")
  ).sort(desc("MEDIA_DIFF_PREV_CHEGADA_MINUTOS"),"media_velocidade_chegada")

  df_velocidade_menor_10_VEICULO = df_velocidade_menor_10.groupBy("ID_VEICULO").agg(
      percentile_approx("velocidade_para_chegar", 0.5).alias("media_velocidade_chegada"),
      percentile_approx("diff_in_minute", 0.5).alias("MEDIA_DIFF_PREV_CHEGADA_MINUTOS"),
      percentile_approx("distancia_km", 0.5).alias("media_distancia_km_PONTOS")
  ).sort(desc("MEDIA_DIFF_PREV_CHEGADA_MINUTOS"),"media_velocidade_chegada")

  tabela_maior_velocidade = "PREV_ATRASADO"
  tabela_maior_parada = f"{tabela_maior_velocidade}_PARADA"
  df_velocidade_maior_40_parada.write.jdbc(url=url, table=tabela_maior_parada, mode="overwrite", properties=properties)

  tabela_maior_parada = f"{tabela_maior_velocidade}_LINHA"
  df_velocidade_maior_40_LINHA.write.jdbc(url=url, table=tabela_maior_parada, mode="overwrite", properties=properties)

  tabela_maior_parada = f"{tabela_maior_velocidade}_VEICULO"
  df_velocidade_maior_40_VEICULO.write.jdbc(url=url, table=tabela_maior_parada, mode="overwrite", properties=properties)

  tabela_menor_velocidade = "PREV_ADIANTADA"
  tabela_menor_parada = f"{tabela_menor_velocidade}_PARADA"
  df_velocidade_menor_10_parada.write.jdbc(url=url, table=tabela_menor_parada, mode="overwrite", properties=properties)

  tabela_menor_LINHA = f"{tabela_menor_velocidade}_LINHA"
  df_velocidade_menor_10_linha.write.jdbc(url=url, table=tabela_menor_LINHA, mode="overwrite", properties=properties)

  tabela_menor_VEICULO = f"{tabela_menor_velocidade}_VEICULO"
  df_velocidade_menor_10_VEICULO.write.jdbc(url=url, table=tabela_menor_VEICULO, mode="overwrite", properties=properties)

  return True


if __name__ == "__main__":
    main()
