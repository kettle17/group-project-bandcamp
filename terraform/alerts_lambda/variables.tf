variable "DB_NAME" {
    type = string
}

variable "DB_PASSWORD" {
    type = string
}

variable "DB_HOST" {
    type = string
}

variable "DB_PORT" {
    type = string
}

variable "DB_USER" {
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


variable AWS_ACCESS_KEY_ID {
    type = string
}

variable AWS_SECRET_ACCESS_KEY {
    type = string
}