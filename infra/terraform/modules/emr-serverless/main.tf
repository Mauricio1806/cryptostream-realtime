# ──────────────────────────────────────────────
# Security Group for EMR Serverless
# ──────────────────────────────────────────────

resource "aws_security_group" "emr" {
  name_prefix = "${var.project}-${var.environment}-emr-"
  description = "Security group for EMR Serverless workers"
  vpc_id      = var.vpc_id

  # Workers need to talk to each other
  ingress {
    description = "All traffic from self"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  # Workers need to reach MSK
  egress {
    description     = "Kafka brokers"
    from_port       = 9092
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [var.msk_security_group_id]
  }

  # Workers need S3 (via VPC endpoint) and other AWS services
  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-emr-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ──────────────────────────────────────────────
# EMR Serverless Application
# ──────────────────────────────────────────────

resource "aws_emrserverless_application" "streaming" {
  name          = "${var.project}-${var.environment}-streaming"
  release_label = var.emr_release_label
  type          = "SPARK"

  initial_capacity {
    initial_capacity_type = "Driver"

    initial_capacity_config {
      worker_count = var.driver_count

      worker_configuration {
        cpu    = var.driver_cpu
        memory = var.driver_memory
      }
    }
  }

  initial_capacity {
    initial_capacity_type = "Executor"

    initial_capacity_config {
      worker_count = var.executor_count

      worker_configuration {
        cpu    = var.executor_cpu
        memory = var.executor_memory
      }
    }
  }

  maximum_capacity {
    cpu    = var.max_cpu
    memory = var.max_memory
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.idle_timeout_minutes
  }

  network_configuration {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.emr.id]
  }

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-emr-streaming"
  })
}
