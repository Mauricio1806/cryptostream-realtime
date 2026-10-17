variable "project" {
  type = string
}

variable "environment" {
  type = string
}

variable "bucket_arns" {
  description = "Map of layer to S3 bucket ARN"
  type        = map(string)
}

variable "bucket_names" {
  description = "Map of layer to S3 bucket name"
  type        = map(string)
}

variable "tags" {
  type    = map(string)
  default = {}
}
