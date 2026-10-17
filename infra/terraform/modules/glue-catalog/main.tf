# ──────────────────────────────────────────────
# Glue Database
# ──────────────────────────────────────────────

resource "aws_glue_catalog_database" "lake" {
  name        = "${var.project}_${var.environment}"
  description = "CryptoStream ${var.environment} data lake catalog"

  tags = var.tags
}

# ──────────────────────────────────────────────
# Glue Crawler IAM Role
# ──────────────────────────────────────────────

resource "aws_iam_role" "glue_crawler" {
  name = "${var.project}-${var.environment}-glue-crawler"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "s3-lake-access"
  role = aws_iam_role.glue_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = flatten([
          for arn in values(var.bucket_arns) : [arn, "${arn}/*"]
        ])
      }
    ]
  })
}

# ──────────────────────────────────────────────
# Glue Crawlers — one per layer
# ──────────────────────────────────────────────

resource "aws_glue_crawler" "silver_trades" {
  database_name = aws_glue_catalog_database.lake.name
  name          = "${var.project}-${var.environment}-silver-trades"
  role          = aws_iam_role.glue_crawler.arn

  delta_target {
    delta_tables = ["s3://${var.bucket_names["silver"]}/trades/"]
    write_manifest = true
  }

  schedule = "cron(0 */6 * * ? *)" # Every 6 hours

  tags = var.tags
}

resource "aws_glue_crawler" "silver_orderbook" {
  database_name = aws_glue_catalog_database.lake.name
  name          = "${var.project}-${var.environment}-silver-orderbook"
  role          = aws_iam_role.glue_crawler.arn

  delta_target {
    delta_tables = ["s3://${var.bucket_names["silver"]}/orderbook/"]
    write_manifest = true
  }

  schedule = "cron(0 */6 * * ? *)"

  tags = var.tags
}

resource "aws_glue_crawler" "gold_vwap" {
  database_name = aws_glue_catalog_database.lake.name
  name          = "${var.project}-${var.environment}-gold-vwap"
  role          = aws_iam_role.glue_crawler.arn

  delta_target {
    delta_tables = [
      "s3://${var.bucket_names["gold"]}/vwap_1m/",
      "s3://${var.bucket_names["gold"]}/vwap_5m/",
      "s3://${var.bucket_names["gold"]}/vwap_15m/",
    ]
    write_manifest = true
  }

  schedule = "cron(0 */6 * * ? *)"

  tags = var.tags
}
