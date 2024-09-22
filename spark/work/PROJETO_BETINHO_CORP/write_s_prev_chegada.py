
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, radians, sin, cos, atan2, sqrt
from pyspark.sql.types import DoubleType

def generate_haversine_calc(dataframe):
  df = df_prev_chegada.withColumn("lat1_rad", radians(col("latitude_loc"))) \
       .withColumn("lon1_rad", radians(col("longtitude"))) \
       .withColumn("lat2_rad", radians(col("LATITUDE_VEICULO"))) \
       .withColumn("lon2_rad", radians(col("LONGITUDE_VEICULO")))

  df = df.withColumn("dlat", col("lat2_rad") - col("lat1_rad")) \
        .withColumn("dlon", col("lon2_rad") - col("lon1_rad"))

  df = df.withColumn("a", sin(col("dlat") / 2)**2 + cos(col("lat1_rad")) * cos(col("lat2_rad")) * sin(col("dlon") / 2)**2)

  df = df.withColumn("c", 2 * atan2(sqrt(col("a")), sqrt(1 - col("a"))))

  df = df.withColumn("distancia_km", col("c") * R)

  df = df.select(
     "dat_ref_carga",
     "id_parada",
     "nome_parada",
     "horario_previsto_cheada",
     "PREFIXO_VEICULO",
     "latitude_loc",
     "longtitude",
     "LATITUDE_VEICULO",
     "LONGITUDE_VEICULO",
     "distancia_km")

  return df

def main() :
  spark = SparkSession.builder \
    .appName('User Raw to Trusted') \
    .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()

  raw_path_previsao_chegada = 's3a://raw/previsao_chegada'

  df_explode = df_previsao_chegada.withColumn("ps_explod", explode("ps"))\
      .withColumn("vs_explod", explode("ps_explod.vs")).select(col("hr").alias("dat_ref_carga"),"ps_explod","vs_explod")

  df_prev_chegada = df_explode.select(
      col("dat_ref_carga"),
      col("ps_explod.cp").alias("ID_PARADA"),
      col("ps_explod.np").alias("nome_parada"),
      col("ps_explod.px").alias("latitude_loc"),
      col("ps_explod.py").alias("longtitude"),
      col("vs_explod.a").alias("VEICULO_ACESSIVEL"),
      col("vs_explod.is").alias("TIMESTAMP"),
      col("vs_explod.p").alias("PREFIXO_VEICULO"),
      col("vs_explod.px").alias("LATITUDE_VEICULO"),
      col("vs_explod.py").alias("LONGITUDE_VEICULO"),
      col("vs_explod.t").alias("HORARIO_PREVISTO_CHEADA")
  )
  silver_path_posicao_veiculo = 's3a://trusted/previsao_chegada'
  df_final = generate_haversine_calc(df_prev_chegada)
  df_final.write.format('delta')\
      .mode('overwrite').option("overwriteSchema", "true").save(silver_path_posicao_veiculo)

  return True


if __name__ == "__main__":
    main()
