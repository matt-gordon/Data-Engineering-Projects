#!/bin/bash
aws emr create-cluster \
--name=project-4-cluster \
--use-default-roles \
--release-label emr-5.28.0 \
--applications Name=Spark \
--ec2-attributes KeyName=spark-cluster \
--instance-type m5.xlarge \
--instance-count 4 \
--configurations file://./configurations.json \
--log-uri s3://emrlogs 
