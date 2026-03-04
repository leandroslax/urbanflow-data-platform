terraform {
  required_version = ">= 1.5.0"

  backend "s3" {
    bucket         = "urbanflow-tfstate-us-east-1-139961319000"
    key            = "urbanflow/dev/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "urbanflow-tf-lock-us-east-1"
    encrypt        = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}
