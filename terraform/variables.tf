variable "project_name" {
  description = "Project name used for naming resources"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "raw_bucket_name" {
  description = "S3 bucket for raw ingested data"
  type        = string
}

variable "tfstate_bucket_name" {
  description = "S3 bucket for Terraform remote state"
  type        = string
}