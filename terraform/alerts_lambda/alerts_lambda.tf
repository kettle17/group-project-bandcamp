

provider "aws" {
    region = var.AWS_REGION
    access_key = var.AWS_ACCESS_KEY_ID
    secret_key = var.AWS_SECRET_ACCESS_KEY
}


data "aws_vpc" "c17-vpc" {
    id = var.VPC_ID
}

######################### 
### SNS topic
#########################

resource "aws_sns_topic" "trending_alerts" {
  name = "c17-tracktion-trending-artists-alerts"
}

######################### 
### ECR
#########################

data "aws_ecr_repository" "alerts-lambda-repo" {
  name = "c17-tracktion-alerts-ecr"
}

data "aws_ecr_image" "alerts-lambda-image" {
  repository_name = data.aws_ecr_repository.alerts-lambda-repo.name
  image_tag       = "latest"
}

#########################
### Security Group
#########################

resource "aws_security_group" "lambda_sg" {
    name        = "c17-tracktion-alerts-lambda-sg"
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

data "aws_iam_policy_document" "alerts-lambda-role-trust-policy-doc" {
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

data "aws_iam_policy_document" "alerts-lambda-role-permissions-policy-doc" {
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

    statement {
    effect = "Allow"
    actions = [
    ["sns:Publish"]
    ]
    resources = [ "arn:aws:sns:eu-west-2:129033205317:c17-tracktion-trending-artists-alerts" ]

    }

}

resource "aws_iam_role" "alerts-lambda-role" {
  name = "c17-tracktion-alerts-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.alerts-lambda-role-trust-policy-doc.json
}

resource "aws_iam_policy" "alerts-lambda-role-permissions-policy" {
  name = "c17-tracktion-alerts-lambda-permissions-policy"
  policy = data.aws_iam_policy_document.alerts-lambda-role-permissions-policy-doc.json
}

resource "aws_iam_role_policy_attachment" "alerts-lambda-role-policy-connection" {
  role = aws_iam_role.alerts-lambda-role.name
  policy_arn = aws_iam_policy.alerts-lambda-role-permissions-policy.arn
}

#########################
### Lambda 
#########################

resource "aws_lambda_function" "alerts-lambda" {
  function_name = "c17-tracktion-alerts-lambda-function"
  role = aws_iam_role.alerts-lambda-role.arn
  package_type = "Image"
  image_uri = data.aws_ecr_image.alerts-lambda-image.image_uri
  timeout = 300
  memory_size = 512
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
    name = "c17-tracktion-alerts-scheduler-role"

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
    name = "c17-tracktion-alerts-scheduler-lambda-invoke"
    role = aws_iam_role.scheduler-role.id

    policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
        Effect = "Allow"
        Action = "lambda:InvokeFunction"
        Resource = [
            aws_lambda_function.alerts-lambda.arn,
        ]
        }
    ]
    })
}

#########################
### EventBridge Schedule 
#########################

resource "aws_scheduler_schedule_group" "alerts-group" {
    name = "c17-tracktion-alerts-scheduler-group"
}

resource "aws_scheduler_schedule" "alerts-schedule" {
    name       = "c17-tracktion-alerts-schedule"
    group_name = aws_scheduler_schedule_group.alerts-group.name

    flexible_time_window {
    mode = "OFF"
    }

    schedule_expression = "cron(0 7 * * ? *)"

    target {
        arn      = aws_lambda_function.alerts-lambda.arn
        role_arn = aws_iam_role.scheduler-role.arn

        input = jsonencode({
            source = "eventbridge-scheduler"
            time   = "scheduled"
        })
    }

    description = "Trigger alerts script once per day."
    state       = "ENABLED"
}

resource "aws_lambda_permission" "allow-alerts-scheduler" {
    statement_id  = "AllowSchedulerInvoke"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.alerts-lambda.function_name
    principal     = "scheduler.amazonaws.com"
    source_arn    = aws_scheduler_schedule.alerts-schedule.arn

}
