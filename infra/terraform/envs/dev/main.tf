provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

locals {
  tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ──────────────────────────────────────────────
# VPC
# ──────────────────────────────────────────────

module "vpc" {
  source = "../../modules/vpc"

  project            = var.project
  environment        = var.environment
  aws_region         = var.aws_region
  vpc_cidr           = var.vpc_cidr
  az_count           = 2 # Dev: 2 AZs to save cost
  enable_nat_gateway = true
  tags               = local.tags
}

# ──────────────────────────────────────────────
# S3 Data Lake
# ──────────────────────────────────────────────

module "s3_lake" {
  source = "../../modules/s3-lake"

  project               = var.project
  environment           = var.environment
  bronze_retention_days  = 90 # Dev: shorter retention
  tags                  = local.tags
}

# ──────────────────────────────────────────────
# MSK (Kafka)
# ──────────────────────────────────────────────

module "msk" {
  source = "../../modules/msk"

  project              = var.project
  environment          = var.environment
  vpc_id               = module.vpc.vpc_id
  vpc_cidr             = module.vpc.vpc_cidr_block
  private_subnet_ids   = module.vpc.private_subnet_ids
  broker_instance_type = "kafka.t3.small"
  broker_count         = 2 # Dev: 2 brokers
  broker_ebs_volume_size = 50
  log_retention_hours  = 24
  tags                 = local.tags
}

# ──────────────────────────────────────────────
# EMR Serverless
# ──────────────────────────────────────────────

module "emr_serverless" {
  source = "../../modules/emr-serverless"

  project               = var.project
  environment           = var.environment
  vpc_id                = module.vpc.vpc_id
  private_subnet_ids    = module.vpc.private_subnet_ids
  msk_security_group_id = module.msk.security_group_id
  executor_count        = 1 # Dev: minimal capacity
  idle_timeout_minutes  = 15
  tags                  = local.tags
}

# ──────────────────────────────────────────────
# Glue Catalog
# ──────────────────────────────────────────────

module "glue_catalog" {
  source = "../../modules/glue-catalog"

  project      = var.project
  environment  = var.environment
  bucket_arns  = module.s3_lake.bucket_arns
  bucket_names = module.s3_lake.bucket_names
  tags         = local.tags
}

# ──────────────────────────────────────────────
# IAM Roles
# ──────────────────────────────────────────────

module "iam" {
  source = "../../modules/iam"

  project             = var.project
  environment         = var.environment
  aws_region          = var.aws_region
  bucket_arns         = module.s3_lake.bucket_arns
  glue_database_name  = module.glue_catalog.database_name
  emr_application_arn = module.emr_serverless.application_arn
  airflow_bucket_arn  = module.mwaa.dag_bucket_arn
  tags                = local.tags
}

# ──────────────────────────────────────────────
# MWAA (Airflow)
# ──────────────────────────────────────────────

module "mwaa" {
  source = "../../modules/mwaa"

  project                = var.project
  environment            = var.environment
  vpc_id                 = module.vpc.vpc_id
  private_subnet_ids     = module.vpc.private_subnet_ids
  mwaa_execution_role_arn = module.iam.mwaa_execution_role_arn
  environment_class      = "mw1.small"
  max_workers            = 2
  tags                   = local.tags
}
