output "vpc_id" {
  value = module.vpc.vpc_id
}

output "msk_bootstrap_brokers" {
  value     = module.msk.bootstrap_brokers
  sensitive = true
}

output "msk_bootstrap_brokers_tls" {
  value     = module.msk.bootstrap_brokers_tls
  sensitive = true
}

output "emr_application_id" {
  value = module.emr_serverless.application_id
}

output "glue_database" {
  value = module.glue_catalog.database_name
}

output "s3_buckets" {
  value = module.s3_lake.bucket_names
}

output "airflow_url" {
  value = module.mwaa.webserver_url
}

output "emr_job_role_arn" {
  value = module.iam.emr_job_role_arn
}
