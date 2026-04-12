# Terraform state bucket
resource "aws_s3_bucket" "tf_state" {
  bucket = var.tfstate_bucket_name


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
