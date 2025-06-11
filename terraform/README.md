# 🏗️ Terraform

This directory contains the Terraform configuration used to provision and manage the AWS infrastructure required to support the ETL pipeline.

---

## ☁️ Provisioned Cloud Resources

The following AWS resources are provisioned:

- **Amazon RDS (PostgreSQL)**  
  A managed relational database instance used to store structured data ingested through the ETL process.
  
- **Security Group**  
  Controls access to the RDS instance (port 5432 open to the internet by default — modify for production purposes).

- **DB Subnet Group**  
  Ensures high availability by deploying the RDS instance across multiple subnets in a VPC.

---

## 📁 File Structure

```bash
terraform/
├── main.tf           # Core Terraform configuration for RDS, networking, and security groups
├── variables.tf      # Input variables for configuration (e.g., database credentials, subnet IDs, etc.)
├── terraform.tfvars  # User-defined values for the variables (should be created manually)
└── README.md         # This file
