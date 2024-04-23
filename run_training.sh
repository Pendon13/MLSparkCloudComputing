pip install -r requirements.txt
hadoop fs -put TrainingDataset.csv
hadoop fs -put ValidationDataset.csv
spark-submit modelTraining.py