output "database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.lake.name
}

output "crawler_role_arn" {
  description = "IAM role ARN for Glue crawlers"
  value       = aws_iam_role.glue_crawler.arn
}
