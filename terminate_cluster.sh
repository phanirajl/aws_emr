#!/bin/bash

sudo useradd dror_user
sudo mkdir /home/dror_user
sudo mkdir /home/dror_user/.aws/
sudo chmod 777 -R /home/dror_user
sudo echo "[default]
aws_access_key_id = AKIAJIGKPBVWNXYWJOFA
aws_secret_access_key = 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" > /home/dror_user/.aws/credentials
sudo echo "[default]
region=us-east-1" > /home/dror_user/.aws/config


clusters=`sudo su dror_user -c 'aws emr list-clusters --cluster-states WAITING --query 'Clusters[*].Id' --output text'`


sudo su dror_user -c 'aws emr terminate-clusters --cluster-ids '$clusters''