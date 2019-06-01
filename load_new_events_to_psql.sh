#!/bin/bash

EC2_INSTANCE_ID="`wget -q -O - http://instance-data/latest/meta-data/instance-id || die \"wget instance-id has failed: $?\"`"
test -n "$EC2_INSTANCE_ID" || die 'cannot obtain instance-id'
EC2_AVAIL_ZONE="`wget -q -O - http://instance-data/latest/meta-data/placement/availability-zone || die \"wget availability-zone has failed: $?\"`"
test -n "$EC2_AVAIL_ZONE" || die 'cannot obtain availability-zone'
EC2_REGION="`echo \"$EC2_AVAIL_ZONE\" | sed -e 's:\([0-9][0-9]*\)[a-z]*\$:\\1:'`"


LOGFILE=/tmp/load_new_events_to_postgresql_$currentDate.log
cd /tmp/
currentDate=`date +"%Y""%m""%d"_"%H""%M""%S"`
LOGFILE=/tmp/load_new_events_to_postgresql_$currentDate.log

rm -rf /tmp/pages /tmp/events

####Save to S3 output folder:
###Export to CSV:
sudo su hdfs -c "hive -e 'select * from powtoon_hive.cloudfront_log_stg' | gzip > /tmp/cloudfront_log_$currentDate.csv.gz"
status=$?
   if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "export cloudfront_log to csv failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR-Import to postgresql failed - export cloudfront_log to csv failed'
   exit 1
else echo $(date +"%D") $(date +"%T") "export cloudfront_log to csv successful" | tee -a $LOGFILE
fi
###Upload CSV to S3:
sudo aws s3 cp /tmp/cloudfront_log_$currentDate.csv.gz s3://powtoon-bi/cloudfront_tracking/powtoon_cloudfront_logs/
status=$?
   if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "aws s3 cp cloudfront_log failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR-Import to postgresql failed - aws s3 cp cloudfront_log failed'
   exit 1
else echo  $(date +"%D") $(date +"%T") "aws s3 cp cloudfront_log successful" | tee -a $LOGFILE
fi

####Save errors to S3 output folder:
###Export to CSV:
sudo su hdfs -c "hive -e 'select * from powtoon_hive.cloudfront_log_stg_errors' | gzip > /tmp/cloudfront_log_errors_$currentDate.csv.gz"
status=$?
   if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "export cloudfront_log_stg_errors to csv failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR-Import to postgresql failed - export cloudfront_log_stg_errors to csv failed'
   exit 1
else echo $(date +"%D") $(date +"%T") "export cloudfront_log_stg_errors to csv successful" | tee -a $LOGFILE
fi
error_rows=$(sudo zcat /tmp/cloudfront_log_errors_$currentDate.csv.gz | wc -l)
###Upload CSV to S3:
if [ $error_rows -ne 0 ]; then
sudo aws s3 cp /tmp/cloudfront_log_errors_$currentDate.csv.gz s3://powtoon-bi/cloudfront_tracking/powtoon_cloudfront_errors/
status=$?
fi
   if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "aws s3 cp cloudfront_log_stg_errors failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR-Import to postgresql failed - aws s3 cp cloudfront_log_stg_errors failed'
   exit 1
else echo  $(date +"%D") $(date +"%T") "aws s3 cp cloudfront_log_stg_errors successful" | tee -a $LOGFILE
fi

mkdir /tmp/pages/
sudo su hdfs -c "hdfs dfs -copyToLocal /user/admin/page/* /tmp/pages/"
status=$?
   if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "export page to csv failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR-Import to postgresql failed - export page to csv failed'
   exit 1
else echo  $(date +"%D") $(date +"%T") "export page to csv successful" | tee -a $LOGFILE
fi


mkdir /tmp/events/
sudo su hdfs -c "hdfs dfs -copyToLocal /user/admin/event/* /tmp/events/"
status=$?
   if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "export event to csv failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR-Import to postgresql failed - export event to csv failed'
   exit 1
else echo $(date +"%D") $(date +"%T") "export event to csv successful" | tee -a $LOGFILE
fi


sudo yum install postgresql -y
status=$?
   if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "yum install postgresql -y failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR-Import to postgresql failed - yum install postgresql -y failed'
   exit 1
else echo $(date +"%D") $(date +"%T") "yum install postgresql -y successful" | tee -a $LOGFILE
fi

echo "BEGIN;" > /tmp/sql_postgresql_inset.sql
files=/tmp/events/*
if [ `find /tmp/events/ | wc -l` -gt 1 ]; then
   for f in $files
   do 
      echo processing $f | tee -a $LOGFILE
      sed -i 's/,""/,/g; s/\x0/ /g' $f 
      echo "\copy public.tracking_events (date,user_id,category,action,label,value,visitor_id,ip,session_id,operating_sys,extra_data_json) from '$f' with (format csv, DELIMITER '|',  QUOTE '\"');" >> /tmp/sql_postgresql_inset.sql
   done
fi

files=/tmp/pages/*
if [ `find /tmp/pages/ | wc -l` -gt 1 ]; then
   for f in $files
   do 
      echo processing $f | tee -a $LOGFILE
      sed -i 's/,""/,/g; s/\x0/ /g' $f 
      echo "\copy public.tracking_pageviews (date,user_id,page_path,referer,tracking_referer,medium,campaign,source,visitor_id,ip,session_id,operating_sys,ad_id,keyword,user_agent) from '$f' with (format csv,DELIMITER '|', QUOTE '\"');" >> /tmp/sql_postgresql_inset.sql
   done
fi

echo "END;" >> /tmp/sql_postgresql_inset.sql
sudo echo 'powtoon-bi.caehxz6ziu8i.us-east-1.rds.amazonaws.com:5432:powtoon_bi:dror:powtoon101' > ~/.pgpass
sudo chmod 600 ~/.pgpass

###Get the Elastic IP 52.72.187.126
#aws ec2 disassociate-address --association-id eipassoc-ef491c94

aws ec2 associate-address --instance-id $EC2_INSTANCE_ID --allow-reassociation --allocation-id eipalloc-37778909
status=$?
   if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "ec2 associate-address failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR-Import to postgresql failed - ec2 associate-address failed'
   exit 1
else echo  $(date +"%D") $(date +"%T") "ec2 associate-address successful" | tee -a $LOGFILE
fi

sleep 1m

psql -hpowtoon-bi.caehxz6ziu8i.us-east-1.rds.amazonaws.com -p5432 -Udror -d powtoon_bi -f /tmp/sql_postgresql_inset.sql > >(tee -a $LOGFILE) 2> >(tee -a $LOGFILE >&2)
status=$?
   if [ $status -ne 0 ] ; then
   echo $(date +"%D") $(date +"%T") "loading to postgresql failed" | tee -a $LOGFILE
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR-Import to postgresql failed - loading to postgresql failed'
   exit 1
fi
   echo $(date +"%D") $(date +"%T") "loading to postgresql successful" | tee -a $LOGFILE

##PSQL doesnt return 1 in case of an error, to deal with it we need to review the log file and return 1 in case of error.
if [ `cat $LOGFILE | grep -v 'cloudfront_log_stg_errors' | grep -i error | wc -l` -ge 1 ] ; then
   errormsg=$(cat "$LOGFILE" | grep -v 'cloudfront_log_stg_errors' | grep -i error -a1)
   echo $errormsg
   aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR-Import to postgresql failed with error: $errormsg'
   exit 1
else aws sns publish --topic-arn arn:aws:sns:us-east-1:873652372660:emr-status --message 'EMR-Import to postgresql successful'
fi