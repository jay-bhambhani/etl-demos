#!/bin/bash
# script to launch and terminate emr cluster on completion



aws emr create-cluster --name OB-Prototype --release-label emr-4.1.0 \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge InstanceGroupType=CORE,InstanceCount=3,InstanceType=m3.xlarge \
--applications Name=Hadoop  Name=Hive Name=Hue Name=Pig Name=Mahout Name=Ganglia Name=Spark\
--use-default-roles --ec2-attributes KeyName=jay ,SubnetId=subnet-42ede904 \
--log-uri s3://cinchfinancial-mortgage-optimalblue-proto/logs \
#--steps Name=SparkPi, Jar=s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar, Args=[/home/hadoop/spark/bin/spark-submit,--deploy-mode,cluster,--master,yarn,--class,org.apache.spark.examples.SparkPi,s3://support.elasticmapreduce/spark/1.2.0/spark-examples-1.2.0-hadoop2.4.0.jar,10],ActionOnFailure="TERMINATE" \
--steps Name=InterestRatesPrototype,Jar=s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar,Args=[/home/hadoop/spark/bin/spark-submit,--deploy-mode,cluster,s3://],ActionOnFailure=CONTINUE
--auto-terminate
