pip install -r requirements.txt
sudo -u hdfs hadoop fs -mkdir /user/ec2-user
sudo -u hdfs hadoop fs -chown ec2-user /user/ec2-user
hadoop fs -put TrainingDataset.csv
hadoop fs -put ValidationDataset.csv
spark-submit modelTraining.py

hadoop fs -copyToLocal /user/ec2-user/job