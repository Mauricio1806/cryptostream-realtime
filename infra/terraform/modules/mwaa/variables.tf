variable "project" {
  type = string
}

variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "mwaa_execution_role_arn" {
  description = "IAM role ARN for MWAA execution"
  type        = string
}

variable "airflow_version" {
  type    = string
  default = "2.8.1"
}

variable "environment_class" {
  description = "MWAA environment class (mw1.small, mw1.medium, mw1.large)"
  type        = string
  default     = "mw1.small"
}

variable "max_workers" {
  type    = number
  default = 2
}

variable "min_workers" {
  type    = number
  default = 1
}

variable "tags" {
  type    = map(string)
  default = {}
}
