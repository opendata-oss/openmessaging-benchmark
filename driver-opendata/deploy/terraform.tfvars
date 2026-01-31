public_key_path = "~/.ssh/opendata_aws.pub"
region          = "us-west-2"
az              = "us-west-2a"
ami             = "ami-0b6d6dacf350ebc82" # Amazon Linux 2023 us-west-2

instance_type  = "m5n.xlarge"
num_instances  = 1

s3_bucket = "jason-opendata-bench"
