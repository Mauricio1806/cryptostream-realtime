# ──────────────────────────────────────────────
# S3 Bucket for DAGs
# ──────────────────────────────────────────────

resource "aws_s3_bucket" "airflow" {
  bucket = "${var.project}-${var.environment}-airflow"

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-airflow"
  })
}

resource "aws_s3_bucket_versioning" "airflow" {
  bucket = aws_s3_bucket.airflow.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "airflow" {
  bucket = aws_s3_bucket.airflow.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ──────────────────────────────────────────────
# Security Group for MWAA
# ──────────────────────────────────────────────

resource "aws_security_group" "mwaa" {
  name_prefix = "${var.project}-${var.environment}-mwaa-"
  description = "Security group for MWAA environment"
  vpc_id      = var.vpc_id

  ingress {
    description = "All traffic from self"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-mwaa-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ──────────────────────────────────────────────
# MWAA Environment
# ──────────────────────────────────────────────

resource "aws_mwaa_environment" "main" {
  name               = "${var.project}-${var.environment}"
  airflow_version    = var.airflow_version
  environment_class  = var.environment_class
  max_workers        = var.max_workers
  min_workers        = var.min_workers

  source_bucket_arn      = aws_s3_bucket.airflow.arn
  dag_s3_path            = "dags/"
  requirements_s3_path   = "requirements.txt"
  execution_role_arn     = var.mwaa_execution_role_arn

  network_configuration {
    subnet_ids         = slice(var.private_subnet_ids, 0, 2) # MWAA needs exactly 2
    security_group_ids = [aws_security_group.mwaa.id]
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "WARNING"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-mwaa"
  })
}
