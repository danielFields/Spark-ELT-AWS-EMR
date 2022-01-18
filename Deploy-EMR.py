import os


create_statement = """
aws emr create-cluster \
--name emr-sandbox \
--use-default-roles \
--release-label emr-5.28.0 \
--instance-count 3 \
--applications Name=Spark Name=Hive  \
--ec2-attributes KeyName=putty-1,SubnetId=subnet-086f716ac1da230fb,EmrManagedMasterSecurityGroup=sg-0465cf83ad1d1d93e,EmrManagedSlaveSecurityGroup=sg-02e120b3b87b8a5f3 \
--instance-type m5.xlarge \
--profile default \
--bootstrap-actions Path=s3://aws-logs-678848124427-us-west-2/boot/pip-install-pyspark.sh \
--no-verify-ssl
"""

os.system(create_statement)

