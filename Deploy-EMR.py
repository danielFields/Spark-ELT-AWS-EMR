import os

create_statement = """
aws emr create-cluster \
--name emr-sandbox \
--use-default-roles \
--release-label emr-5.28.0 \
--instance-count 3 \
--applications Name=Spark  \
--ec2-attributes KeyName=[DOWNLOADED-KEY.(pem|ppk)],SubnetId=[SUBNET-IN-ACCOUNT],EmrManagedMasterSecurityGroup=[EMR-MANAGED SG FOR MASTER NODE],EmrManagedSlaveSecurityGroup=[EMR-MANAGED SG FOR WORKER NODE(S)] \
--instance-type m5.xlarge \
--profile default \
--bootstrap-actions Path=s3://aws-logs-[ACCOUNT-ID]-us-west-2/boot/pip-install-pyspark.sh \
--no-verify-ssl
"""

os.system(create_statement)

