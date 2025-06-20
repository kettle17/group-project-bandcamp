 Trending Artists Alerts Script

This project identifies **trending artists** based on a revenue increase of over 50% from two days ago to yesterday, using sales data stored in a PostgreSQL database. If any are found, an alert message is automatically sent via **AWS SNS**.

---

## 📁 File Structure

`alerts.py ` - Main script to generate and send alerts

`requirements.txt` - Python dependencies

`.env` - Environment variables (not committed)

`Dockerfile` - Container build definition

⚙️ Prerequisites

Before running or deploying the script, ensure the following infrastructure is set up:

	1.	Amazon RDS instance set up and populated with Bandcamp sales data.
	2.	Amazon ECR (Elastic Container Registry):
	•	Used to host the Docker image.
	•	You must build and push the image before deploying the Lambda function.



# Set up

Set up your .env 

DB_HOST=your-rds-endpoint
DB_PORT=5432
DB_USER=your-db-username
DB_PASSWORD=your-db-password
DB_NAME=your-db-name


install dependencies 
`pip install -r requirements.txt`


This directory:
- Queries the RDS database for revenue data over the past 2 days
- Filters for artists with >50% revenue increase
- Publishes an alert message to the configured SNS topic

If no artists meet the criteria, the script exits with:
No trending artists over the past 2 days.
