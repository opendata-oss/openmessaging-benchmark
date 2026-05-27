#!/bin/bash
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

set -e

cd "$(dirname "$0")"

echo "==> Generating Ansible inventory from Terraform..."
echo "[client]" > hosts.ini
terraform output -json client_public_ips | jq -r '.[]' >> hosts.ini
echo "Created hosts.ini with $(wc -l < hosts.ini | xargs) host(s)"

echo ""
echo "==> Reading Terraform outputs..."
S3_BUCKET=$(terraform output -raw s3_bucket)
REGION=$(terraform output -raw region)
OPENDATA_BRANCH=$(terraform output -raw opendata_branch)
OPENDATA_JAVA_BRANCH=$(terraform output -raw opendata_java_branch)
BENCHMARK_BRANCH=$(terraform output -raw benchmark_branch)
SEPARATE_READER=$(terraform output -raw separate_reader)
READ_VISIBILITY=$(terraform output -raw read_visibility)
READER_REFRESH_INTERVAL_MS=$(terraform output -raw reader_refresh_interval_ms)
TELEMETRY_ENABLED=$(terraform output -raw telemetry_enabled)
TELEMETRY_LOG_FILTER=$(terraform output -raw telemetry_log_filter)

echo "    s3_bucket:                  $S3_BUCKET"
echo "    region:                     $REGION"
echo "    opendata_branch:            $OPENDATA_BRANCH"
echo "    opendata_java_branch:       $OPENDATA_JAVA_BRANCH"
echo "    benchmark_branch:           $BENCHMARK_BRANCH"
echo "    separate_reader:            $SEPARATE_READER"
echo "    read_visibility:            $READ_VISIBILITY"
echo "    reader_refresh_interval_ms: $READER_REFRESH_INTERVAL_MS"
echo "    telemetry_enabled:          $TELEMETRY_ENABLED"
echo "    telemetry_log_filter:       $TELEMETRY_LOG_FILTER"

echo ""
echo "==> Running Ansible playbook..."
ansible-playbook deploy.yaml \
  -e "s3_bucket=$S3_BUCKET" \
  -e "region=$REGION" \
  -e "opendata_branch=$OPENDATA_BRANCH" \
  -e "opendata_java_branch=$OPENDATA_JAVA_BRANCH" \
  -e "benchmark_branch=$BENCHMARK_BRANCH" \
  -e "separate_reader=$SEPARATE_READER" \
  -e "read_visibility=$READ_VISIBILITY" \
  -e "reader_refresh_interval_ms=$READER_REFRESH_INTERVAL_MS" \
  -e "telemetry_enabled=$TELEMETRY_ENABLED" \
  -e "telemetry_log_filter=$TELEMETRY_LOG_FILTER" \
  "$@"
