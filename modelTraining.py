import findspark
findspark.init()
findspark.find()

import pyspark
import numpy as np
import pandas as pd

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import col

from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier, LinearSVC
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#import findspark
#findspark.init()
#findspark.find()

# Options
trainingPath = "TrainingDataset.csv"
treeCount = 1000
maxIterCount = 1000

#Starting the spark session
#spark = SparkSession.builder.master("local[*]").getOrCreate()
conf = pyspark.SparkConf().setAppName('winequality').setMaster('local')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.1.3')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

#Loading the training dataset
train_df = spark.read.options(header = True ,sep =';', quote="", dtype='float').csv(trainingPath)
#df.printSchema()
#df.show()

#Removing quotes & setting features to float
for col_name in train_df.columns:
    train_df = train_df.withColumn(col_name, col(col_name).cast('float'))
    train_df = train_df.withColumnRenamed(col_name, col_name.replace('"',''))
train_df = train_df.withColumnRenamed('quality','label')
#df.printSchema()
#df.show()

#print("Choose Feature columns not quality column")
feature_cols = [x for x in train_df.columns if x != "label"]
#print(feature_cols)

#print("Create and configure the assembler")
assembler = VectorAssembler(inputCols=feature_cols, 
                            outputCol='features')
# transform the original data
dataDF = assembler.transform(train_df)
# dataDF.printSchema()
# dataDF.show()
data = dataDF.select(['features','label'])
# data.show()

#print("Scale the data")
scaler = StandardScaler().setInputCol('features').setOutputCol('scaled_features')



#print("Generate RandomForestClassifier with " + str(treeCount) + " trees")
rf = RandomForestClassifier(labelCol="label", featuresCol="scaled_features", numTrees=treeCount)

#print("Generate GBTClassifier with " + str(maxIterCount) + " iterations")
#gbt = GBTClassifier(labelCol="label", featuresCol="scaled_features", maxIter=maxIterCount)

#print("Generate LinearSVC with " + str(maxIterCount) + " iterations")
#lsvc = LinearSVC(labelCol="label", featuresCol="scaled_features", maxIter=maxIterCount, regParam=0.1)

#print("Create Pipe - Assemble, Scale, Model")
pipe = Pipeline(stages=[assembler, scaler, rf])

#print("Split for cross validation")
(trainingData, testData) = train_df.randomSplit([0.7, 0.3])

#print("Train Model")
model = pipe.fit(trainingData)


#print("Make predictions.")
predictions = model.transform(testData)

#print("Select example rows to display.")
#predictions.select("prediction", "label", "features").show(5)

#print("Select (prediction, true label) and compute test error")
evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction"
)

pred_accuracy = evaluator.evaluate(
    predictions, 
    {evaluator.metricName: "accuracy"}
)

pred_f1 = evaluator.evaluate(
    predictions, 
    {evaluator.metricName: "f1"}
)

#file = open("./job/model.txt", 'a')
#file.write("\n")
#file.write("Test Error = %g\n" % (1.0 - pred_accuracy))
#file.write("Accuracy = %g\n" % pred_accuracy)
#file.write("F1 score = %g\n" % pred_f1)

print("Test Error = %g" % (1.0 - pred_accuracy))
print("Accuracy = %g" % pred_accuracy)
print("F1 score = %g" % pred_f1)

# Saving the Pipeline model results 
model.write().overwrite().save("./job/Modelfile")
#model.write().overwrite().save('s3://aws-logs-590184008894-us-east-1/elasticmapreduce/mlspark/Modelfile')