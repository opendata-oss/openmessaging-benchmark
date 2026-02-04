public_key_path = "~/.ssh/opendata_aws.pub"
region          = "us-west-2"
az              = "us-west-2a"
ami             = "ami-0b6d6dacf350ebc82" # Amazon Linux 2023 us-west-2

instance_type  = "m5n.xlarge"
num_instances  = 1

s3_bucket = "jason-opendata-bench"
existing_vpc_id = "vpc-0cce609162e73f3f8"
existing_subnet_id = "subnet-0ca81290f8c673111"
associate_public_ip = true
opendata_branch = "fix-storage-reader"