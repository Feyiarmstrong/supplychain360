output "raw_data_bucket_name" {
  description = "Name of the raw data S3 bucket"
  value       = aws_s3_bucket.raw_data.bucket
}

output "raw_data_bucket_arn" {
  description = "ARN of the raw data S3 bucket"
  value       = aws_s3_bucket.raw_data.arn
}

output "tfstate_bucket_name" {
  description = "Name of the Terraform state S3 bucket"
  value       = aws_s3_bucket.tf_state.bucket
}