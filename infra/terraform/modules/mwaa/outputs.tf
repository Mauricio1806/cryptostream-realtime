output "environment_arn" {
  description = "MWAA environment ARN"
  value       = aws_mwaa_environment.main.arn
}

output "webserver_url" {
  description = "Airflow webserver URL"
  value       = aws_mwaa_environment.main.webserver_url
}

output "dag_bucket_name" {
  description = "S3 bucket for DAGs"
  value       = aws_s3_bucket.airflow.id
}

output "dag_bucket_arn" {
  description = "S3 bucket ARN for DAGs"
  value       = aws_s3_bucket.airflow.arn
}
