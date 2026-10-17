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

variable "msk_security_group_id" {
  type = string
}

variable "emr_release_label" {
  description = "EMR release label (e.g. emr-7.0.0 for Spark 3.5)"
  type        = string
  default     = "emr-7.0.0"
}

variable "driver_count" {
  type    = number
  default = 1
}

variable "driver_cpu" {
  type    = string
  default = "2 vCPU"
}

variable "driver_memory" {
  type    = string
  default = "4 GB"
}

variable "executor_count" {
  type    = number
  default = 2
}

variable "executor_cpu" {
  type    = string
  default = "2 vCPU"
}

variable "executor_memory" {
  type    = string
  default = "4 GB"
}

variable "max_cpu" {
  type    = string
  default = "16 vCPU"
}

variable "max_memory" {
  type    = string
  default = "64 GB"
}

variable "idle_timeout_minutes" {
  description = "Minutes of idle before auto-stop (streaming jobs should set high)"
  type        = number
  default     = 60
}

variable "tags" {
  type    = map(string)
  default = {}
}
