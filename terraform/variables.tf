variable "environment" {
  type        = string
  description = "The deployment environment (e.g., dev, stg, prd)."

  validation {
    condition     = contains(["dev", "stg", "prod"], var.environment)
    error_message = "Environment must be dev, stg, or prod."
  }
}

variable "bucket_tier" {
  type        = string
  description = "The deployment environment (e.g., raw, staging, production)."

  validation {
    condition     = contains(["raw", "staging", "production"], var.bucket_tier)
    error_message = "Tier must be raw, staging, or production."
  }
}

variable "aws_region" {
  type        = string
  description = "The AWS region to deploy resources in."
  default     = "us-east-1"
}

variable "aws_account_id" {
  type = string
  description = "The AWS Account ID used to create the infrastructure, change it to your own"
  default = "552738622317"
}