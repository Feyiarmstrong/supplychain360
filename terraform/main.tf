
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         = "supplychain360-tfstate-feyisayo"
    key            = "terraform/state/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "supplychain360-tf-locks"
    encrypt        = true
    profile        = "default"
  }
}

provider "aws" {
  region  = "us-east-1"
  profile = "default"
}