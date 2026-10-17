variable "project" {
  type = string
}

variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "vpc_cidr" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "kafka_version" {
  type    = string
  default = "3.6.0"
}

variable "broker_count" {
  description = "Number of broker nodes (must be multiple of AZ count)"
  type        = number
  default     = 3
}

variable "broker_instance_type" {
  type    = string
  default = "kafka.t3.small"
}

variable "broker_ebs_volume_size" {
  description = "EBS volume size in GB per broker"
  type        = number
  default     = 100
}

variable "log_retention_hours" {
  description = "Kafka log retention in hours"
  type        = number
  default     = 168
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 14
}

variable "tags" {
  type    = map(string)
  default = {}
}
