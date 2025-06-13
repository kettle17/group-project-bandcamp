data "aws_vpc" "c17_vpc"{
    id = var.VPC_ID
}

resource "aws_db_instance" "tracktion_rds_db" {
    identifier                   = "c17-tracktion-rds"
    allocated_storage            = 20
    db_name                      = var.DB_NAME
    engine                       = "postgres"
    engine_version               = "17"
    instance_class               = "db.t3.micro"
    publicly_accessible          = true
    performance_insights_enabled = false
    skip_final_snapshot          = true
    db_subnet_group_name         = aws_db_subnet_group.tracktion_db_subnet_group.name
    vpc_security_group_ids       = [aws_security_group.tracktion_db_security_group.id]
    username                     = var.DB_USERNAME
    password                     = var.DB_PASSWORD
}

resource "aws_db_subnet_group" "tracktion_db_subnet_group" {
  name       = "c17-tracktion-db-subnet-grp"
  subnet_ids = var.AWS_SUBNETS
}

resource "aws_vpc_security_group_ingress_rule" "postgres_inbound_rule"{
    security_group_id = aws_security_group.tracktion_db_security_group.id
    cidr_ipv4 = "0.0.0.0/0"
    from_port = 5432
    ip_protocol = "tcp"
    to_port = 5432
}

resource "aws_security_group" "tracktion_db_security_group" {
    name = "c17-tracktion-db-sec-grp"
    vpc_id = data.aws_vpc.c17_vpc.id
}
