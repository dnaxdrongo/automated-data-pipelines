param(
  [string]$BucketName = "dbadle1-wgu-udacity",
  [string]$S3Prefix   = "data-pipelines",
  [string]$Region     = "us-east-1"
)

# Run from the airflow-project folder:
#   powershell -ExecutionPolicy Bypass -File .\set_connections.ps1 -BucketName dbadle1-wgu-udacity

Write-Host "Setting Airflow Variables: s3_bucket=$BucketName, s3_prefix=$S3Prefix"
docker compose exec -T airflow-webserver airflow variables set s3_bucket $BucketName
docker compose exec -T airflow-webserver airflow variables set s3_prefix $S3Prefix

# Optional: Create the AWS connection from environment variables
if ($env:AWS_ACCESS_KEY_ID -and $env:AWS_SECRET_ACCESS_KEY) {
  $extra = "{`"region_name`":`"$Region`"}"
  Write-Host "Creating Airflow connection aws_credentials from env vars (Region=$Region)"
  docker compose exec -T airflow-webserver airflow connections delete aws_credentials 2>$null
  docker compose exec -T airflow-webserver airflow connections add aws_credentials `
    --conn-type aws `
    --conn-login $env:AWS_ACCESS_KEY_ID `
    --conn-password $env:AWS_SECRET_ACCESS_KEY `
    --conn-extra $extra
}
else {
  Write-Host "Skipping aws_credentials creation (set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to enable)."
}

Write-Host "Done."
