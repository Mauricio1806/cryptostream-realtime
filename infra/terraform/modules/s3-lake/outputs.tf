output "bucket_arns" {
  description = "Map of layer name to bucket ARN"
  value       = { for k, v in aws_s3_bucket.lake : k => v.arn }
}

output "bucket_names" {
  description = "Map of layer name to bucket name"
  value       = { for k, v in aws_s3_bucket.lake : k => v.id }
}

output "bronze_bucket_arn" {
  value = aws_s3_bucket.lake["bronze"].arn
}

output "silver_bucket_arn" {
  value = aws_s3_bucket.lake["silver"].arn
}

output "gold_bucket_arn" {
  value = aws_s3_bucket.lake["gold"].arn
}

output "checkpoints_bucket_arn" {
  value = aws_s3_bucket.lake["checkpoints"].arn
}
