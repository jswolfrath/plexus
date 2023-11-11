#sudo apt install default-jdk scala git
#wget https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
#mv spark-3.3.0-bin-hadoop3 spark

#wget https://archive.apache.org/dist/kafka/3.2.1/kafka_2.13-3.2.1.tgz

# Setup ramdisk for quick file transfer
sudo mkdir -p /media/join
sudo mount -t tmpfs -o size=2048M tmpfs /media/join
sudo chmod 777 /media/join
