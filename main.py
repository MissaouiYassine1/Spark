from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession


#Créer une session Spark
spark = SparkSession.builder.appName("Spark").getOrCreate()

# Charger un fichier CSV en DataFrame
df = spark.read.csv("ml_data.csv", header=True, inferSchema=True) 

''' Assurez-vous que le fichier 
data.csv est dans le même répertoire que ce script ou fournissez le chemin complet 
sinon on obtient une erreur : pyspark.errors.exceptions.captured.AnalysisException '''

# Préparer les données
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
data = assembler.transform(df)

# Entraîner un modèle de régression logistique
lr = LogisticRegression(featuresCol="features", labelCol="label")
model = lr.fit(data)

predictions = model.transform(data)
predictions.select("features", "label", "prediction",'probability').show()

# Exécuter une requête SQL
""" df.createOrReplaceTempView("data")
result = spark.sql("SELECT region, AVG(revenue) FROM data GROUP BY region") """

'''En cette requête, nous sélectionnons la région et la moyenne des revenus, 
puis nous groupons les résultats par région. Si votre fichier CSV ne contient pas
 des colonnes nommées "region" et "revenue", vous devrez ajuster la requête SQL 
 en fonction des noms de colonnes réels de votre fichier CSV.
Sinon on obtiendra une erreur : 
pyspark.errors.exceptions.captured.AnalysisException . 
'''

# Afficher le résultat
""" result.show() """

input("Appuyez sur Entrée pour quitter...")