sudo yum install docker -y
sudo systemctl start docker
sudo docker pull pendon/mlspark:latest
spark-submit modelTraining.py
docker run --rm --entrypoint /bin/sh image_name -c "cat /path/filename" > output_filename
