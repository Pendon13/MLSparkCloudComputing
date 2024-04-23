aws s3 cp s3://aws-logs-590184008894-us-east-1/elasticmapreduce/mlspark/modelTraining.py ./
aws s3 cp s3://aws-logs-590184008894-us-east-1/elasticmapreduce/mlspark/TrainingDataset.csv ./
aws s3 cp s3://aws-logs-590184008894-us-east-1/elasticmapreduce/mlspark/ValidationDataset.csv ./
spark-submit modelTraining.py