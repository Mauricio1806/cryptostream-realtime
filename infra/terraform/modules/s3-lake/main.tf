# ──────────────────────────────────────────────
# Data Lake Buckets
# ──────────────────────────────────────────────

locals {
  buckets = {
    bronze      = "${var.project}-${var.environment}-bronze"
    silver      = "${var.project}-${var.environment}-silver"
    gold        = "${var.project}-${var.environment}-gold"
    checkpoints = "${var.project}-${var.environment}-checkpoints"
  }
}

resource "aws_s3_bucket" "lake" {
  for_each = local.buckets
  bucket   = each.value

  tags = merge(var.tags, {
    Name  = each.value
    Layer = each.key
  })
}

resource "aws_s3_bucket_versioning" "lake" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.lake[each.key].id

  versioning_configuration {
    status = each.key == "checkpoints" ? "Disabled" : "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.lake[each.key].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.lake[each.key].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ──────────────────────────────────────────────
# Lifecycle Rules
# ──────────────────────────────────────────────

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.lake["bronze"].id

  rule {
    id     = "transition-to-ia"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = var.bronze_retention_days
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "checkpoints" {
  bucket = aws_s3_bucket.lake["checkpoints"].id

  rule {
    id     = "cleanup-old-checkpoints"
    status = "Enabled"

    expiration {
      days = 30
    }
  }
}
