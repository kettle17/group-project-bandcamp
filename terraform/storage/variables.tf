variable "DB_NAME" {
    type = string
}

variable "DB_USERNAME" {
  type = string
}

variable "DB_PASSWORD" {
    type = string
}

variable "AWS_SUBNETS" {
  type = list(string)
}

variable "VPC_ID" {
  type = string
}

variable "AWS_REGION" {
    type = string
    default = "eu-west-2"
}