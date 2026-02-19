from pyspark.sql import SparkSession


#Créer une session Spark
spark = SparkSession.builder.appName("Spark").getOrCreate()

# Charger un fichier CSV en DataFrame
df = spark.read.csv("data.csv", header=True, inferSchema=True) 

''' Assurez-vous que le fichier 
data.csv est dans le même répertoire que ce script ou fournissez le chemin complet 
sinon on obtient une erreur : pyspark.errors.exceptions.captured.AnalysisException '''

# Exécuter une requête SQL
df.createOrReplaceTempView("data")
result = spark.sql("SELECT region, AVG(revenue) FROM data GROUP BY region")

'''En cette requête, nous sélectionnons la région et la moyenne des revenus, 
puis nous groupons les résultats par région. Si votre fichier CSV ne contient pas
 des colonnes nommées "region" et "revenue", vous devrez ajuster la requête SQL 
 en fonction des noms de colonnes réels de votre fichier CSV.
Sinon on obtiendra une erreur : 
pyspark.errors.exceptions.captured.AnalysisException . 
'''

# Afficher le résultat
result.show()

input("Appuyez sur Entrée pour quitter...")