from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col


def main():
    spark = SparkSession.builder \
        .appName('User Raw to Trusted') \
        .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog') \
        .getOrCreate()

    posicao_veiculo = 's3a://raw/posicao_veiculo'

    df_posicao_veiculo = spark.read.json(posicao_veiculo)

    df_posicao_veiculo2 = df_posicao_veiculo.withColumn("dict", explode(col('l')))
    df_posicao_veiculo3 = df_posicao_veiculo2.select(
        col("dict.c").alias("LETREIRO_LINHA"), 
        col("dict.cl").alias("ID_LINHA"), 
        col("dict.lt0").alias("LETREIRO_DESTINO_LINHA"), 
        col("dict.lt1").alias("LETREIRO_ORIGEM_LINHA"), 
        col("dict.qv").alias("QTD_VEICULO_LINHA"), 
        col("dict.sl").alias("SENTIDO_O_LINHA"), 
        explode("dict.vs").alias("veiculos"),
        col("veiculos.a").alias("ACESSIVEL_VEICULO"),
        col("veiculos.p").alias("ID_VEICULO"),
        col("veiculos.px").alias("LONGITUDE_VEICULO"),
        col("veiculos.py").alias("LATITUDE_VEICULO")
    ).drop("veiculos")

    silver_path_posicao_veiculo = 's3a://trusted/posicao_veiculo'

    df_posicao_veiculo3.write.format('delta')\
        .mode('overwrite').option("overwriteSchema", "true").save(silver_path_posicao_veiculo)
    
    return print("RODOU LEGAL")

if __name__ == "__main__":
    main()