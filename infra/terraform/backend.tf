terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.30"
    }
  }

  # Each environment configures its own backend via -backend-config
  # Example:
  #   terraform init -backend-config="bucket=cryptostream-terraform-state" \
  #                  -backend-config="key=prod/terraform.tfstate" \
  #                  -backend-config="region=us-east-1" \
  #                  -backend-config="dynamodb_table=cryptostream-terraform-locks"
  backend "s3" {}
}
