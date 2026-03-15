variable "project_name" {
  description = "Project name used for naming resources"
  type        = string
  default     = "supplychain360"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "raw_bucket_name" {
  description = "S3 bucket for raw ingested data"
  type        = string
  default     = "supplychain360-raw-feyisayo"
}

variable "tfstate_bucket_name" {
  description = "S3 bucket for Terraform remote state"
  type        = string
  default     = "supplychain360-tfstate-feyisayo"
}