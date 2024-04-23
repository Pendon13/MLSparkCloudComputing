import pyspark, sys
import numpy as np
import pandas as pd
import os

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import col

from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier, LinearSVC
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#import findspark
#findspark.init()
#findspark.find()

# Options
testingPath = sys.argv[1]
treeCount = 1000
maxIterCount = 1000

#Starting the spark session
conf = pyspark.SparkConf().setAppName('winequality').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

#Loading the training dataset
test_df = spark.read.options(header = True ,sep =';', quote="", dtype='float').csv(testingPath)
#df.printSchema()
#df.show()

#Removing quotes & setting features to float
for col_name in test_df.columns:
    test_df = test_df.withColumn(col_name, col(col_name).cast('float'))
    test_df = test_df.withColumnRenamed(col_name, col_name.replace('"',''))
test_df = test_df.withColumnRenamed('quality','label')
#df.printSchema()
#df.show()

#print("Choose Feature columns not quality column")
feature_cols = [x for x in test_df.columns if x != "label"]
#print(feature_cols)

#print("Create and configure the assembler")
assembler = VectorAssembler(inputCols=feature_cols, 
                            outputCol='features')
# transform the original data
dataDF = assembler.transform(test_df)
# dataDF.printSchema()
# dataDF.show()
data = dataDF.select(['features','label'])
model = PipelineModel.load("./job/Modelfile")
#model = RandomForestClassifier.load("./job/Modelfile")

#print("Make predictions.")
predictions = model.transform(test_df)

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

file = open("model.txt", 'w')

file.write("Test Error = %g\n" % (1.0 - pred_accuracy))
file.write("Accuracy = %g\n" % pred_accuracy)
file.write("F1 score = %g\n" % pred_f1)

print("Test Error = %g\n" % (1.0 - pred_accuracy))
print("Accuracy = %g\n" % pred_accuracy)
print("F1 score = %g\n" % pred_f1)

rfModel = model.stages[2]
print(rfModel)  # summary only