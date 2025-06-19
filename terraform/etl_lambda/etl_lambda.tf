
provider "aws" {
    region = var.AWS_REGION
    access_key = var.AWS_ACCESS_KEY_ID
    secret_key = var.AWS_SECRET_ACCESS_KEY
}


data "aws_vpc" "c17-vpc" {
    id = var.VPC_ID
}

######################### 
### ECR
#########################


data "aws_ecr_repository" "pipeline-lambda-repo" {
  name = "c17-tracktion-etl-ecr"
}

data "aws_ecr_image" "pipeline-lambda-image" {
  repository_name = data.aws_ecr_repository.pipeline-lambda-repo.name
  image_tag       = "latest"
}

#########################
### Security Group
#########################

resource "aws_security_group" "lambda_sg" {
    name        = "c17-tracktion-lambda-sg"
    description = "Security group for Lambda functions"
    vpc_id      = data.aws_vpc.c17-vpc.id

    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }
}


######################### 
### IAM Lambda
#########################

data "aws_iam_policy_document" "lambda-role-trust-policy-doc" {
    statement {
      effect = "Allow"
      principals {
        type = "Service"
        identifiers = [ "lambda.amazonaws.com" ]
      }
      actions = [
        "sts:AssumeRole"
      ]
    }
}

data "aws_iam_policy_document" "etl-lambda-role-permissions-policy-doc" {
    statement {
      effect = "Allow"
      actions = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      resources = [ "arn:aws:logs:eu-west-2:129033205317:*" ]
    }

    statement {
      effect = "Allow"
      actions = [
        "rds-db:connect",
        "rds:*"
      ]
      resources = [ "arn:aws:rds-db:eu-west-2:129033205317:dbuser:c17-tracktion-rds/public" ]
    }

}

resource "aws_iam_role" "etl-lambda-role" {
  name = "c17-tracktion-etl-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda-role-trust-policy-doc.json
}

resource "aws_iam_policy" "etl-lambda-role-permissions-policy" {
  name = "c17-tracktion-etl-lambda-permissions-policy"
  policy = data.aws_iam_policy_document.etl-lambda-role-permissions-policy-doc.json
}

resource "aws_iam_role_policy_attachment" "etl-lambda-role-policy-connection" {
  role = aws_iam_role.etl-lambda-role.name
  policy_arn = aws_iam_policy.etl-lambda-role-permissions-policy.arn
}

#########################
### Lambda 
#########################

resource "aws_lambda_function" "etl-lambda" {
  function_name = "c17-tracktion-etl-lambda-function"
  role = aws_iam_role.etl-lambda-role.arn
  package_type = "Image"
  image_uri = data.aws_ecr_image.pipeline-lambda-image.image_uri
  timeout = 480
  reserved_concurrent_executions = 1
  environment {
    variables = {
        DB_HOST = var.DB_HOST
        DB_USER = var.DB_USER
        DB_PASSWORD = var.DB_PASSWORD
        DB_NAME = var.DB_NAME
        DB_PORT = var.DB_PORT
    } 
  }
}


#########################
### IAM EventBridge
#########################

resource "aws_iam_role" "scheduler-role" {
    name = "c17-tracktion-scheduler-role"

    assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
            Service = "scheduler.amazonaws.com"
        }
        }
    ]
    })
}

resource "aws_iam_role_policy" "scheduler_lambda_policy" {
    name = "c17-tracktion-scheduler-lambda-invoke"
    role = aws_iam_role.scheduler-role.id

    policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
        Effect = "Allow"
        Action = "lambda:InvokeFunction"
        Resource = [
            aws_lambda_function.etl-lambda.arn,
        ]
        }
    ]
    })
}

#########################
### EventBridge Schedule 
#########################

resource "aws_scheduler_schedule_group" "etl-group" {
    name = "c17-tracktion-etl-scheduler-group"
}

resource "aws_scheduler_schedule" "etl-schedule" {
    name       = "c17-tracktion-etl-schedule"
    group_name = aws_scheduler_schedule_group.etl-group.name

    flexible_time_window {
    mode = "OFF"
    }

    schedule_expression = "rate(2 minutes)"

    target {
        arn      = aws_lambda_function.etl-lambda.arn
        role_arn = aws_iam_role.scheduler-role.arn

        input = jsonencode({
            source = "eventbridge-scheduler"
            time   = "scheduled"
        })
    }

    description = "Trigger ETL pipeline every two minutes."
    state       = "ENABLED"
}

resource "aws_lambda_permission" "allow-etl-scheduler" {
    statement_id  = "AllowSchedulerInvoke"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.etl-lambda.function_name
    principal     = "scheduler.amazonaws.com"
    source_arn    = aws_scheduler_schedule.etl-schedule.arn

}
