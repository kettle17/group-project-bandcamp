

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

data "aws_ecr_repository" "report-lambda-repo" {
  name = "c17-tracktion-report-ecr"
}

data "aws_ecr_image" "report-lambda-image" {
  repository_name = data.aws_ecr_repository.report-lambda-repo.name
  image_tag       = "latest"
}

#########################
### Security Group
#########################

resource "aws_security_group" "lambda_sg" {
    name        = "c17-tracktion-report-lambda-sg"
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

data "aws_iam_policy_document" "report-lambda-role-permissions-policy-doc" {
    statement {
      effect = "Allow"
      actions = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      resources = [ "arn:aws:logs:eu-west-2:129033205317:log-group:aws/lambda/*" ]
    }

    statement {
      effect = "Allow"
      actions = [
        "rds-db:connect",
        "rds:*"
      ]
      resources = [ "arn:aws:rds-db:eu-west-2:129033205317:dbuser:c17-tracktion-rds/public" ]
    }

      statement {
      effect = "Allow"
      actions = [
        "s3:PutObject"
      ]
      resources = [ "arn:aws:rds-db:eu-west-2:129033205317:dbuser:c17-tracktion-rds/public" ]
    }


}

resource "aws_iam_role" "report-lambda-role" {
  name = "c17-tracktion-report-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda-role-trust-policy-doc.json
}

resource "aws_iam_policy" "report-lambda-role-permissions-policy" {
  name = "c17-tracktion-report-lambda-permissions-policy"
  policy = data.aws_iam_policy_document.report-lambda-role-permissions-policy-doc.json
}

resource "aws_iam_role_policy_attachment" "report-lambda-role-policy-connection" {
  role = aws_iam_role.report-lambda-role.name
  policy_arn = aws_iam_policy.report-lambda-role-permissions-policy.arn
}

#########################
### Lambda 
#########################

resource "aws_lambda_function" "report-lambda" {
  function_name = "c17-tracktion-report-lambda-function"
  role = aws_iam_role.report-lambda-role.arn
  package_type = "Image"
  image_uri = data.aws_ecr_image.report-lambda-image.image_uri
  timeout = 60
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
    name = "c17-tracktion-report-scheduler-role"

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
    name = "c17-tracktion-report-scheduler-lambda-invoke"
    role = aws_iam_role.scheduler-role.id

    policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
        Effect = "Allow"
        Action = "lambda:InvokeFunction"
        Resource = [
            aws_lambda_function.report-lambda.arn,
        ]
        }
    ]
    })
}

#########################
### EventBridge Schedule 
#########################

resource "aws_scheduler_schedule_group" "report-group" {
    name = "c17-tracktion-report-scheduler-group"
}

resource "aws_scheduler_schedule" "report-schedule" {
    name       = "c17-tracktion-report-schedule"
    group_name = aws_scheduler_schedule_group.report-group.name

    flexible_time_window {
    mode = "OFF"
    }

    schedule_expression = "rate(1 day)"

    target {
        arn      = aws_lambda_function.report-lambda.arn
        role_arn = aws_iam_role.scheduler-role.arn

        input = jsonencode({
            source = "eventbridge-scheduler"
            time   = "scheduled"
        })
    }

    description = "Trigger report script once per day."
    state       = "ENABLED"
}

resource "aws_lambda_permission" "allow-report-scheduler" {
    statement_id  = "AllowSchedulerInvoke"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.report-lambda.function_name
    principal     = "scheduler.amazonaws.com"
    source_arn    = aws_scheduler_schedule.report-schedule.arn

}
