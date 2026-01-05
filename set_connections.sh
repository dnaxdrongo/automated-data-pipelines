#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   BUCKET_NAME=dbadle1-wgu-udacity S3_PREFIX=data-pipelines ./set_connections.sh
#
# This script sets Airflow Variables (and optionally the AWS connection) inside the running
# docker-compose Airflow environment so they can be recreated quickly.

BUCKET_NAME="${BUCKET_NAME:-dbadle1-wgu-udacity}"
S3_PREFIX="${S3_PREFIX:-data-pipelines}"
AWS_REGION="${AWS_REGION:-us-east-1}"

# Allow override if you use 'docker-compose' (v1) instead of 'docker compose' (v2)
COMPOSE_CMD="${COMPOSE_CMD:-docker compose}"

echo "Setting Airflow Variables: s3_bucket=${BUCKET_NAME}, s3_prefix=${S3_PREFIX}"
$COMPOSE_CMD exec -T airflow-webserver airflow variables set s3_bucket "${BUCKET_NAME}"
$COMPOSE_CMD exec -T airflow-webserver airflow variables set s3_prefix "${S3_PREFIX}"

# Optional: Create/overwrite the AWS connection from environment variables.
# NOTE: This will pass credentials through the CLI. Prefer setting via the Airflow UI if you want
#       to avoid putting secrets on your shell history.
if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  echo "Creating Airflow connection aws_credentials from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (region ${AWS_REGION})"
  $COMPOSE_CMD exec -T airflow-webserver airflow connections delete aws_credentials >/dev/null 2>&1 || true
  $COMPOSE_CMD exec -T airflow-webserver airflow connections add aws_credentials \
    --conn-type aws \
    --conn-login "${AWS_ACCESS_KEY_ID}" \
    --conn-password "${AWS_SECRET_ACCESS_KEY}" \
    --conn-extra "{\"region_name\": \"${AWS_REGION}\"}"
else
  echo "Skipping aws_credentials creation (set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to enable)."
fi

echo "Done."
