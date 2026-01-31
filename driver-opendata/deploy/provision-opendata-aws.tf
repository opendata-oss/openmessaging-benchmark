#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

provider "aws" {
  region  = var.region
  version = "~> 3.0"
}

provider "random" {
  version = "~> 3.0"
}

variable "public_key_path" {
  description = "Path to SSH public key for authentication"
}

variable "key_name" {
  default     = "opendata-benchmark-key"
  description = "Desired name prefix for the AWS key pair"
}

variable "region" {
  description = "AWS region"
}

variable "az" {
  description = "AWS availability zone"
}

variable "ami" {
  description = "AMI ID for EC2 instances"
}

variable "instance_type" {
  description = "EC2 instance type for benchmark clients"
  default     = "m5n.xlarge"
}

variable "num_instances" {
  description = "Number of benchmark client instances"
  default     = 1
}

resource "random_id" "hash" {
  byte_length = 8
}

# VPC
resource "aws_vpc" "benchmark_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "OpenData_Benchmark_VPC_${random_id.hash.hex}"
  }
}

# Internet gateway
resource "aws_internet_gateway" "benchmark_gw" {
  vpc_id = aws_vpc.benchmark_vpc.id

  tags = {
    Name = "OpenData_Benchmark_GW_${random_id.hash.hex}"
  }
}

# Route table
resource "aws_route" "internet_access" {
  route_table_id         = aws_vpc.benchmark_vpc.main_route_table_id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.benchmark_gw.id
}

# Subnet
resource "aws_subnet" "benchmark_subnet" {
  vpc_id                  = aws_vpc.benchmark_vpc.id
  cidr_block              = "10.0.0.0/24"
  map_public_ip_on_launch = true
  availability_zone       = var.az

  tags = {
    Name = "OpenData_Benchmark_Subnet_${random_id.hash.hex}"
  }
}

# Security group
resource "aws_security_group" "benchmark_security_group" {
  name   = "opendata-benchmark-${random_id.hash.hex}"
  vpc_id = aws_vpc.benchmark_vpc.id

  # SSH access
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Benchmark worker HTTP (for distributed mode)
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  # All ports open within VPC
  ingress {
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  # Outbound internet access (for S3, package downloads)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "OpenData_Benchmark_SG_${random_id.hash.hex}"
  }
}

# SSH key pair
resource "aws_key_pair" "auth" {
  key_name   = "${var.key_name}-${random_id.hash.hex}"
  public_key = file(var.public_key_path)
}

# IAM role for S3 access
resource "aws_iam_role" "benchmark_role" {
  name = "opendata-benchmark-role-${random_id.hash.hex}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name = "OpenData_Benchmark_Role_${random_id.hash.hex}"
  }
}

# IAM policy for S3 access
resource "aws_iam_role_policy" "benchmark_s3_policy" {
  name = "opendata-benchmark-s3-policy"
  role = aws_iam_role.benchmark_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket}",
          "arn:aws:s3:::${var.s3_bucket}/*"
        ]
      }
    ]
  })
}

variable "s3_bucket" {
  description = "S3 bucket for OpenData/SlateDB storage"
}

# Instance profile
resource "aws_iam_instance_profile" "benchmark_profile" {
  name = "opendata-benchmark-profile-${random_id.hash.hex}"
  role = aws_iam_role.benchmark_role.name
}

# Benchmark client instances
resource "aws_instance" "client" {
  ami                    = var.ami
  instance_type          = var.instance_type
  key_name               = aws_key_pair.auth.id
  subnet_id              = aws_subnet.benchmark_subnet.id
  vpc_security_group_ids = [aws_security_group.benchmark_security_group.id]
  iam_instance_profile   = aws_iam_instance_profile.benchmark_profile.name
  count                  = var.num_instances

  tags = {
    Name      = "opendata_client_${count.index}"
    Benchmark = "OpenData"
  }
}

# Outputs
output "client_ssh_host" {
  value = aws_instance.client[0].public_ip
}

output "client_public_ips" {
  value = aws_instance.client[*].public_ip
}

output "client_private_ips" {
  value = aws_instance.client[*].private_ip
}

output "s3_bucket" {
  value = var.s3_bucket
}

output "region" {
  value = var.region
}
