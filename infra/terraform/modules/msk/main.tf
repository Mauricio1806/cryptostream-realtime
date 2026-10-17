# ──────────────────────────────────────────────
# Security Group for MSK
# ──────────────────────────────────────────────

resource "aws_security_group" "msk" {
  name_prefix = "${var.project}-${var.environment}-msk-"
  description = "Security group for MSK cluster"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Kafka plaintext from VPC"
    from_port       = 9092
    to_port         = 9092
    protocol        = "tcp"
    cidr_blocks     = [var.vpc_cidr]
  }

  ingress {
    description     = "Kafka TLS from VPC"
    from_port       = 9094
    to_port         = 9094
    protocol        = "tcp"
    cidr_blocks     = [var.vpc_cidr]
  }

  ingress {
    description     = "Kafka IAM auth from VPC"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    cidr_blocks     = [var.vpc_cidr]
  }

  ingress {
    description = "Zookeeper from VPC"
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-msk-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ──────────────────────────────────────────────
# CloudWatch Log Group
# ──────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.project}-${var.environment}"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# ──────────────────────────────────────────────
# MSK Configuration
# ──────────────────────────────────────────────

resource "aws_msk_configuration" "main" {
  name              = "${var.project}-${var.environment}-config"
  kafka_versions    = [var.kafka_version]

  server_properties = <<-PROPERTIES
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=3
    log.retention.hours=${var.log_retention_hours}
    log.segment.bytes=1073741824
    message.max.bytes=1048576
    compression.type=lz4
  PROPERTIES
}

# ──────────────────────────────────────────────
# MSK Cluster
# ──────────────────────────────────────────────

resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.project}-${var.environment}"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_ebs_volume_size
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-msk"
  })
}
