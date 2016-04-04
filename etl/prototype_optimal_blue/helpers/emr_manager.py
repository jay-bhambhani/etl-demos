import os
import sys
from logging import getLogger, basicConfig, INFO

from boto.emr.bootstrap_action import BootstrapAction
from boto.emr.step import InstallHiveStep
from boto.emr.step import InstallPigStep
from boto.regioninfo import RegionInfo
from connectors.aws_connector import AWSConnector

CLUSTER_REGION = 'us-west-2'
CLUSTER_ZONE = 'us-west-2a'
LOG_LOCATION = "S3://cinch-prototype-optimal-blue/cluster_logs/"

class EMRManager(AWSConnector):

    class emr_cluster():

    def __init__(self, cluster_name, cluster_region=CLUSTER_REGION, cluster_zone=CLUSTER_ZONE,
                    ami_version="latest", keep_alive = True, log_location = LOG_LOCATION, 
                    tags={"key":"value"}, hive_version = "1.0.0", impala_version = "1.2.4", bootstrap_script = "", 
                    ec2_keyname = "", pem_key = "", debugging = True, action_on_failure = "CANCEL_AND_WAIT", 
                    master_instance_type = "m3.xlarge", slave_instance_type = "m3.xlarge", num_instances = 4):
        """
        This is a AWS EMR Cluster class. Used to spin up, interact with and terminate an Amazon Web Service EMR cluster.
        PARAMETERS:
            cluster_name - (str) Name of the cluster. As it will appear in the AWS EMR Console cluster list
            cluster_region - (str) AWS compute region
            cluster_zone - (str) AWS compute zone
            cluster_market - (str) cluster market [on demand or spot]
            spot_price - (str) spot bid price
            ami_version - (str) version of AMI to be used
            hadoop_version - (str) version of hadoop to be used
            keep_alive - (bool) keep the cluster running when jobflow is finished
            log_location - (str) s3 location to stash the cluster logs
            tags - (dict) key:value pairs of cluster tags
            hive_version - (str) version of Hive to be installed
            impala_version - (str) version of impala to be installed
            bootstrap_script - (str) directory to shell script to be run at cluster bootstrap ['~/bash.sh']
            ec2_keyname - (str) cluster security key name
            pem_key - (str) location of pem key file ['~/key.pem']
            debugging - (bool) enable cluster debugging
            failure_action - (str) step to take if step fails
            master_type - (str) ec2 instance type
            core_type - (str) ec2 instance type
            num_core_nodes - (int) 
        METHODS:
            check_pem_key - checks to see if the pem key exists in the specified location
            check_cluster_exists - checks to see if a cluster with the given name is already running
            load_cluster - if the cluster exists, connect to that cluster
            set_instance_group - compiles the cluster instance group
            set_cluster_steps - compiles the list of steps
            start_cluster - starts the cluster request
            get_connection - sets up the connection to aws
            get_cluster_status - returns the cluster's current status
            get_cluster_dns - returns the cluster's master public DNS
            get_cluster_ssh - returns a ssh client object to the cluster's master instance
            kill_cluster - terminates the cluster
        """