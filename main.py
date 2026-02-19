from pyspark.sql import SparkSession


#Créer une session Spark
spark = SparkSession.builder.appName("Spark").getOrCreate()


input("Appuyez sur Entrée pour quitter...")