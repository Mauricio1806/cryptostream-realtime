variable "project" {
  type = string
}

variable "environment" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "bucket_arns" {
  description = "Map of layer to S3 bucket ARN"
  type        = map(string)
}

variable "glue_database_name" {
  type = string
}

variable "emr_application_arn" {
  type = string
}

variable "airflow_bucket_arn" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
