#!/bin/bash
hadoop jar /usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar --src=s3://powtoon-prod-access-logs/tracking/prod --dest=hdfs:///user/admin/powtoon_cloudfront_logs --groupBy=".*E3HU2WYLMFI1BA.([0-9]+-[0-9]+-[0-9]+).*" --targetSize=128 --outputCodec=lzo --deleteOnSuccess