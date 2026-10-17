output "emr_job_role_arn" {
  description = "IAM role ARN for EMR Serverless job execution"
  value       = aws_iam_role.emr_job.arn
}

output "mwaa_execution_role_arn" {
  description = "IAM role ARN for MWAA execution"
  value       = aws_iam_role.mwaa_execution.arn
}
