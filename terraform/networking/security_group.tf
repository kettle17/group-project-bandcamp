resource "aws_security_group" "tracktion_db_security_group" {
    name = "c17-tracktion-db-sec-grp"
    vpc_id = data.aws_vpc.c17_vpc.id
}

resource "aws_vpc_security_group_ingress_rule" "postgres_inbound_rule"{
    security_group_id = aws_security_group.tracktion_db_security_group.id
    cidr_ipv4 = "0.0.0.0/0"
    from_port = 5432
    ip_protocol = "tcp"
    to_port = 5432
}
