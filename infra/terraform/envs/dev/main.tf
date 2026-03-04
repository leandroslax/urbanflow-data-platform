locals {
  common_tags = {
    Project     = "urbanflow-data-platform"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket" "datalake" {
  bucket = "${var.project}-datalake-${var.environment}-${var.aws_region}-${var.account_id}"

  tags = local.common_tags
}

resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
