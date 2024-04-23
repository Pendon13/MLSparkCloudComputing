sudo yum install docker -y
sudo systemctl start docker
sudo docker pull pendon/mlspark:version2
sudo docker run -v /home/ec2-user/:job pendon/mlspark:latest