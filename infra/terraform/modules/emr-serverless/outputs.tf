output "application_id" {
  description = "EMR Serverless application ID"
  value       = aws_emrserverless_application.streaming.id
}

output "application_arn" {
  description = "EMR Serverless application ARN"
  value       = aws_emrserverless_application.streaming.arn
}

output "security_group_id" {
  description = "EMR Serverless security group ID"
  value       = aws_security_group.emr.id
}
