from pyspark.sql import SparkSession
import requests
spark = SparkSession.builder.appName('Aula').enableHiveSupport().getOrCreate()

def loadData (qtde):
    list = []
    for x in range (qtde):
        print(x)
        r = requests.get('https://random-data-api.com/api/v2/users')
        list.append(r.json())
        req = spark.read.json(spark.sparkContext.parallelize(list))
        req = req.select( \
         'email' \
        ,'first_name' \
        ,'last_name' \
        ,'gender' \
        ,'id' \
        ,'username' \
                 )
    return req

df = loadData(20)
df.write.parquet('s3a://aula-raw/api/user',mode='append')