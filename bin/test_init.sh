#!/bin/bash

source /opt/init.env

function replace () {
   perl -p -e "s|$1|$2|g" -i $3
}

replace driver_memory $driver_memory /opt/zeppelin/conf/zeppelin-env.sh
replace executor_memory $executor_memory /opt/zeppelin/conf/zeppelin-env.sh
replace notebook_s3_bucket $notebook_s3_bucket /opt/zeppelin/conf/zeppelin-env.sh
replace notebook_s3_user notebook_s3_user /opt/zeppelin/conf/zeppelin-env.sh
replace zeppelin_allowed_origins $zeppelin_allowed_origins /opt/zeppelin/conf/zeppelin-env.sh
replace ego_users $ego_users /opt/zeppelin/conf/shiro.ini
replace ego_public_key_url $ego_public_key_url /opt/zeppelin/conf/shiro.ini
replace ego_admin_role $ego_admin_role /opt/zeppelin/conf/shiro.ini
replace ego_admin_role $ego_login_url /opt/zeppelin/conf/shiro.ini
replace kf_dwh_saved_sets $kf_dwh_saved_sets /opt/spark/conf/spark-defaults.conf
replace kf_dwh_acls $kf_dwh_acls /opt/spark/conf/spark-defaults.conf
replace kf_spark_reverse_proxy_url $kf_spark_reverse_proxy_url /opt/spark/conf/spark-defaults.conf
replace kf_spark_secret $kf_spark_secret /opt/spark/conf/spark-defaults.conf

/opt/spark/sbin/start-master.sh --webui-port 8082

chown -R notebook:notebook /opt/zeppelin/conf
systemctl enable zeppelin.service
systemctl start zeppelin.service

systemctl enable jupyterhub.service
systemctl start jupyterhub.service

