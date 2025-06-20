
This directory contains the script for running an automated reporting pipeline that analyses music sales, identifies top performing artists and genres, and delivers clear, formatted PDF summaries to AWS S3. It is built with Python, Docker, and AWS Lambda for scalable, serverless execution.

## 📁 File Structure

report/
`queries.py` - Queries data from the RDS PostgreSQL database
`pdf_class.py` - Defines the PDF structure and styling (via FPDF)
`report.py` - Core logic to generate the report, upload to S3
`Dockerfile` - Dockerfile for containerising the report script
`tracktion_logo_transparent_light.png` - Logo image used in the PDF header


This directory:
- Connects to a PostgreSQL RDS instance
- Executes SQL queries to fetch sales data
- Generates a structured PDF using fpdf
- Adds formatted text, tables, images (logo), and summary insights
- Uploads the final report to an S3 bucket
- Can be triggered as a Lambda function using a prebuilt Docker image


Prerequisites
	1.	Amazon RDS instance set up and populated with Bandcamp sales data.
	2.	Amazon S3 bucket to store the generated reports
	3.	Amazon ECR (Elastic Container Registry):
	•	Used to host the Docker image.
	•	You must build and push the image before deploying the Lambda function.
