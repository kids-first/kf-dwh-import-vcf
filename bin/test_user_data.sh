#!/bin/bash
sudo cat <<EOT > /opt/init.env
driver_memory=2G
executor_memory=2G
notebook_s3_bucket=kf-strides-variant-parquet-prd
notebook_s3_user=notebooks/c2f93d0f-0d3d-43e8-98e8-ab7a6f7fedb0/notebook/
zeppelin_allowed_origins=http://localz.kidsfirstdrc.org:9100
ego_users=c2f93d0f-0d3d-43e8-98e8-ab7a6f7fedb0
ego_public_key_url=https://ego.kf-strides.org/oauth/token/public_key
ego_admin_role=USER
ego_login_url=https://portal.kidsfirstdrc.org
kf_dwh_saved_sets=s3a://kf-strides-variant-parquet-prd/save_sets_prd/c2f93d0f-0d3d-43e8-98e8-ab7a6f7fedb0
kf_spark_secret=kf_spark_secret
kf_spark_reverse_proxy_url=http://localz.kidsfirstdrc.org:9100
kf_dwh_acls='{"SD_7NQ9151J":[],"SD_BHJXBDQK":["c1"],"SD_NMVV8A1Y":[]}'
EOT
sudo chmod +x /opt/init.sh
sudo /opt/init.sh