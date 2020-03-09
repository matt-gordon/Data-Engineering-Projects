import boto3
import configparser
import json
import time
from botocore.exceptions import ClientError

# Script based upon demos/exercises in Data Engineering Nanodegree
config = configparser.ConfigParser()
config.read_file(open('dwh_notebook.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

# Create client for s3, iam and redshift
region = "us-west-2"

iam = boto3.client('iam',
                    region_name=region,
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET,
                  )

redshift = boto3.client('redshift',
                       region_name=region,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

ClusterIdentifier = redshift.describe_clusters()['Clusters'][0]['ClusterIdentifier']

# Shutdown the Redshift cluster
print("Starting Redshift Cluster Shutdown...")
redshift.delete_cluster( ClusterIdentifier=ClusterIdentifier,  SkipFinalClusterSnapshot=True)

# Wait until the cluster is shutdown
myClusterProps = redshift.describe_clusters(ClusterIdentifier=ClusterIdentifier)['Clusters'][0]
while myClusterProps['ClusterStatus'] == 'deleting':
    time.sleep(15)
    print("Shutting Down Cluster")
    try:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=ClusterIdentifier)['Clusters'][0]
    except Exception as e:
        break
print("Cluster Shutdown")

# Delete the created resources
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)


