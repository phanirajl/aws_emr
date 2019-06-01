#!/bin/bash

# Get AWS CLI version and upgrade
pip install awscli --upgrade --user
status=$?
if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "Upgrade of AWS CLI failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR - Load to Athena failed - Upgrade of AWS CLI failed'
   exit 1
else echo  $(date +"%D") $(date +"%T") "Upgrade of AWS CLI successful" | tee -a $LOGFILE
fi

currentDate=`date +"%Y""%m""%d"_"%H""%M""%S"`
mkdir /tmp/partitioned

# Copy partitioned files from HDFS to local directory
mkdir /tmp/partitioned/events
sudo su hdfs -c "hdfs dfs -copyToLocal /user/admin/events_partitioned/* /tmp/partitioned/events"
status=$?
if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "Copy hive partitions of events to local directory failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR - Load to Athena failed - Copy hive partitions of events to local directory failed'
   exit 1
else echo  $(date +"%D") $(date +"%T") "Copy hive partitions of events to local directory successful" | tee -a $LOGFILE
fi

# Rename copied files and synchronization to S3 bucket
hive_events_files=`{ find /tmp/partitioned/events/ -type f | xargs ls -1t; }`
if [ `{ find /tmp/partitioned/events/ -type f | xargs ls -1t; } | wc -l` -gt 0 ]; then
	for fileName in $hive_events_files
	do 
		sudo mv "$fileName" "${fileName}_$currentDate"
	done
   	aws s3 sync /tmp/partitioned/events s3://powtoon-bi/Athena_data/powtoon_hive/Athena_events
fi
status=$?
if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "Syncing hive partitions files of events failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR - Load to Athena failed - Syncing hive partitions files of events failed'
   exit 1
else echo  $(date +"%D") $(date +"%T") "Syncing hive partitions files of events successful" | tee -a $LOGFILE
fi

# Copy partitioned files from HDFS to local directory
mkdir /tmp/partitioned/pages
sudo su hdfs -c "hdfs dfs -copyToLocal /user/admin/pages_partitioned/* /tmp/partitioned/pages"
status=$?
if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "Copy hive partitions of pages to local directory failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR - Load to Athena failed - Copy hive partitions of pages to local directory failed'
   exit 1
else echo  $(date +"%D") $(date +"%T") "Copy hive partitions of pages to local directory successful" | tee -a $LOGFILE
fi

# Rename copied files and synchronization to S3 bucket
hive_pages_files=`{ find /tmp/partitioned/pages/ -type f | xargs ls -1t; }`
if [ `{ find /tmp/partitioned/pages/ -type f | xargs ls -1t; } | wc -l` -gt 0 ]; then
	for fileName in $hive_pages_files
	do 
		sudo mv "$fileName" "${fileName}_$currentDate"
	done
	aws s3 sync /tmp/partitioned/pages s3://powtoon-bi/Athena_data/powtoon_hive/Athena_pages
fi
status=$?
if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "Syncing hive partitions files of pages failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR - Load to Athena failed - Syncing hive partitions files of pages failed'
   exit 1
else echo  $(date +"%D") $(date +"%T") "Syncing hive partitions files of pages successful" | tee -a $LOGFILE
fi

# Loading all partitions in Athena for events
aws athena start-query-execution --query-string "MSCK REPAIR TABLE powtoon_hive.cloudfront_logs_event_ath;" --result-configuration OutputLocation=s3://aws-athena-query-results-873652372660-us-east-1/
status=$?
if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "Loading all partitions of events failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR - Load to Athena failed - Loading all partitions of events failed'
   exit 1
else echo  $(date +"%D") $(date +"%T") "Loading all partitions of events successful" | tee -a $LOGFILE
fi

# Loading all partitions in Athena for pages
aws athena start-query-execution --query-string "MSCK REPAIR TABLE powtoon_hive.cloudfront_logs_page_ath;" --result-configuration OutputLocation=s3://aws-athena-query-results-873652372660-us-east-1/
status=$?
if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "Loading all partitions of pages failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR - Load to Athena failed - Loading all partitions of pages failed'
   exit 1
else echo  $(date +"%D") $(date +"%T") "Loading all partitions of pages successful" | tee -a $LOGFILE
fi
