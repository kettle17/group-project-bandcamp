# 🏗️ Terraform

This directory contains the Terraform configuration used to provision and manage the AWS infrastructure required to support the ETL pipeline.

---

## 📁 File Structure

```bash
terraform/
├── main.tf           # Core Terraform configuration for RDS, networking, and security groups
├── variables.tf      # Input variables for configuration (e.g., database credentials, subnet IDs, etc.)
└── terraform.tfvars  # User-defined values for the variables (should be created manually)
```
---

## 🔧 Prerequisites

**Important**: This Terraform configuration requires existing VPC and subnet infrastructure. You will need:

- An existing VPC in your AWS account.
- At least 2 subnets in different Availability Zones within that VPC (required for RDS subnet groups).

### Creating VPC Infrastructure
If you don't have existing VPC infrastructure, you can create it using:

- [AWS VPC Creation Guide](https://docs.aws.amazon.com/vpc/latest/userguide/create-vpc.html)
- [AWS VPC Terraform module](https://registry.terraform.io/modules/terraform-aws-modules/vpc/aws/latest)

### Finding Your Existing Infrastructure
To find your VPC and subnet IDs in the AWS Console:

- **VPC ID**: AWS Console → VPC → Your VPCs (format: `vpc-xxxxxxxxx`)
- **Subnet IDs**: AWS Console → VPC → Subnets (format: `subnet-xxxxxxxxx`)

---

## Setting Up Cloud Infrastructure with Terraform

First, create a `terraform.tfvars` file with the following required variables:

```
DB_NAME     = "your-db-name"
DB_USERNAME = "your-db-master-username"
DB_PASSWORD = "your-db-password"
VPC_ID      = "your-vpc-id"
AWS_SUBNETS = [
  "your-subnet-id-1",
  "your-subnet-id-2"
]
```
Then, run the following Terraform commands:
```bash
terraform init
terraform plan
terraform apply
```
Type 'yes' when prompted, this will provision the AWS resources listed below.

---

## ☁️ Cloud Resources

The following AWS resources are provisioned when terraform is ran successfully:

- **Amazon RDS (PostgreSQL)**  
  A managed relational database instance used to store structured data ingested through the ETL process.
  
- **Security Group**  
  Controls access to the RDS instance (port 5432 open to the internet by default — modify for production purposes).

- **DB Subnet Group**  
  Ensures high availability by deploying the RDS instance across multiple subnets in a VPC.

---