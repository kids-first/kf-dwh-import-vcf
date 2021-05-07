#!/bin/bash
sudo cat <<EOT > /opt/init.env
kf_spark_secret=kf_spark_secret
EOT
sudo chmod +x /opt/init.sh
sudo /opt/init.sh
sudo /opt/spark/sbin/start-slave.sh spark://10.12.3.228:7077