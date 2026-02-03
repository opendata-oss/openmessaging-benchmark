# OpenData Benchmark Deployment

Deploy OpenMessaging Benchmark for OpenData on AWS EC2.

## Prerequisites

- AWS CLI configured with credentials
- Terraform >= 1.0
- Ansible >= 2.9
- SSH key pair

## Setup

1. **Create SSH key pair** (if needed):

```bash
ssh-keygen -t rsa -b 4096 -f ~/.ssh/opendata_aws
```

2. **Create S3 bucket** for OpenData/SlateDB:

```bash
aws s3 mb s3://opendata-benchmark --region us-west-2
```

3. **Update terraform.tfvars** with your settings:

```hcl
public_key_path = "~/.ssh/opendata_aws.pub"
region          = "us-west-2"
az              = "us-west-2a"
s3_bucket       = "your-bucket-name"
```

### Using an Existing VPC with S3 Gateway (Optional)

For better S3 performance and reduced data transfer costs, you can deploy into an existing subnet that has an S3 VPC Gateway Endpoint configured:

```hcl
public_key_path    = "~/.ssh/opendata_aws.pub"
region             = "us-west-2"
az                 = "us-west-2a"
s3_bucket          = "your-bucket-name"
existing_vpc_id    = "vpc-0123456789abcdef0"
existing_subnet_id = "subnet-0123456789abcdef0"
```

When these variables are set, Terraform skips creating networking resources (VPC, subnet, internet gateway) and deploys directly into your existing infrastructure.

## Deploy

```bash
cd driver-opendata/deploy

# Provision infrastructure
terraform init
terraform apply

# Create Ansible inventory from Terraform output
echo "[client]" > hosts.ini
terraform output -json client_public_ips | jq -r '.[]' >> hosts.ini

# Add private IPs as host vars
for ip in $(terraform output -json client_public_ips | jq -r '.[]'); do
  priv=$(terraform output -json client_private_ips | jq -r ".[$(grep -n $ip hosts.ini | cut -d: -f1 | head -1) - 2]")
  sed -i '' "s/$ip/$ip private_ip=$priv/" hosts.ini
done

# Deploy benchmark (pass S3 config from Terraform)
ansible-playbook deploy.yaml \
  -e "s3_bucket=$(terraform output -raw s3_bucket)" \
  -e "region=$(terraform output -raw region)"
```

## Run Benchmark

SSH to the client:

```bash
ssh -i ~/.ssh/opendata_aws ec2-user@$(terraform output -raw client_ssh_host)
```

Run a benchmark:

```bash
cd /opt/benchmark

# Run benchmark (uses LocalWorker mode - no separate worker process needed)
sudo -E bin/benchmark --drivers driver-opendata.yaml workloads/1-topic-1-partition-1kb.yaml
```

Note: `sudo -E` is needed to access the native library and preserve environment variables.

## Cleanup

```bash
terraform destroy
aws s3 rb s3://opendata-benchmark --force
```

## Instance Types

|    Type     | vCPU | Memory |    Network    |      Use Case       |
|-------------|------|--------|---------------|---------------------|
| m5n.xlarge  | 4    | 16 GB  | Up to 25 Gbps | Basic testing       |
| m5n.2xlarge | 8    | 32 GB  | Up to 25 Gbps | Standard benchmarks |
| m5n.4xlarge | 16   | 64 GB  | Up to 25 Gbps | High throughput     |

Network bandwidth matters most since all I/O goes through S3.
