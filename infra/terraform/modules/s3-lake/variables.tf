variable "project" {
  type = string
}

variable "environment" {
  type = string
}

variable "bronze_retention_days" {
  description = "Number of days to retain bronze data before expiration"
  type        = number
  default     = 365
}

variable "tags" {
  type    = map(string)
  default = {}
}
