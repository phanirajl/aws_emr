

aws emr create-cluster --name "powtoon cluster" --release-label emr-4.6.0 --use-default-roles \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m4.xlarge InstanceGroupType=CORE,InstanceCount=2,InstanceType=m4.xlarge \
--ec2-attributes KeyName=powtoon-bi,SubnetId=subnet-206aeb0a \
--applications Name=Hadoop Name=Hive Name=Hue \
--log-uri s3://powtoon-bi/logs/emr/ \
--steps \
Type=CUSTOM_JAR,Name=import_to_hdfs,ActionOnFailure=CANCEL_AND_WAIT,Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://powtoon-bi/cloudfront_tracking/scripts/import_s3_to_hdfs.sh \
Type=Hive,Name=CreateHiveTables,ActionOnFailure=CANCEL_AND_WAIT,Args=-f,s3://powtoon-bi/cloudfront_tracking/scripts/HiveCreateTable.sql \
Type=CUSTOM_JAR,Name=import_to_postgresql,ActionOnFailure=CANCEL_AND_WAIT,Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://powtoon-bi/cloudfront_tracking/scripts/load_new_events_to_psql.sh \
Type=CUSTOM_JAR,Name=load_into_athena,ActionOnFailure=CANCEL_AND_WAIT,Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://powtoon-bi/cloudfront_tracking/scripts/load_into_athena.sh \
Type=CUSTOM_JAR,Name=terminate_cluster,ActionOnFailure=CANCEL_AND_WAIT,Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://powtoon-bi/cloudfront_tracking/scripts/terminate_cluster.sh