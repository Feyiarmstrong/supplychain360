# Terraform state bucket
resource "aws_s3_bucket" "tf_state" {
  bucket = var.tfstate_bucket_name

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Name        = "${var.project_name}-tfstate"
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_s3_bucket_versioning" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

 # Raw data bucket
resource "aws_s3_bucket" "raw_data" {
  bucket = var.raw_bucket_name

  tags = {
    Name        = "${var.project_name}-raw"
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Folder structure inside raw bucket
resource "aws_s3_object" "raw_folders" {
  for_each = toset([
    "products/",
    "warehouses/",
    "suppliers/",
    "shipments/",
    "inventory/",
    "store_sales/",
  ])

  bucket = aws_s3_bucket.raw_data.id
  key    = each.value
 }