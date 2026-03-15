
resource "aws_dynamodb_table" "tf_locks" {
  name         = "supplychain360-tf-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Name        = "${var.project_name}-tf-locks"
    Environment = var.environment
    Project     = var.project_name
  }
}