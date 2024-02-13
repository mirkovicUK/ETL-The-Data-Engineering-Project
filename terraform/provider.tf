provider "aws" {
    region = "eu-west-2"
}

terraform { 
    required_providers {
      aws = {
        source = "hashicorp/aws"
        version = "5.7.0"
      }
    }

    backend "s3" {
      bucket = "terraform-state-uros232582"
      key = "tf-state"
      region = "eu-west-2"
    }
}