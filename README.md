# 🎵🎧 Tracktion 🎧🎵
Tracktion is a service that will use the BandCamp API to monitor artist and song analytics using a cloud-based data pipeline and dashboard to monitor this information, send reports and alerts to musicians and labels, and enable data-driven decisions via interactive visualisations.

## 🚰 ETL Pipeline
The ETL pipeline will ingest BandCamp sales data, transform it into structured data, and load it into a cloud-hosted RDS database for storage and analysis. The pipeline will run on a containerised infrastructure using Docker and Terraform.

## 📈 Visualisation
The data is visualised using Streamlit, a lightweight web app framework chosen for its cost effectiveness and customisation capabilities. Developers can adapt the dashboards to their own needs, offering flexibility in how insights are displayed.

## 📍 Folders and Files
```bash
./
├── alerts/               # Folder that contains scripts for the alerts systems and dockerfile.
├── documentation/        # ERD, architecture diagram, WBS breakdown, dashboard wireframe, and user stories.
├── pipeline/             # ETL scripts and related tests.
├── sales_report/         # Script to generate daily pdf report and upload to s3 bucket.
├── streamlit/            # Folder that contains the Streamlit dashboard setup, containing Python page files.
├── terraform/            # Infrastructure-as-code files, including Docker configuration for the ETL pipeline.
├── README.md             # This file!
├── requirements.txt      # Text file containing external library names which are required for the project.  
├── setup.sh              # A Bash script to create a virtual environment and install all dependencies listed in requirements.txt.
```

## Setting Up the Environment
- Run the command `source setup.sh` from the project root directory.

## 🤐 Environment Variable Structure
The following are the variables required in the .env file with their placeholders:
```
DB_USER=your-user-name
DB_PASSWORD=your-db-password
DB_HOST=your-db-host-ip
DB_PORT=your-db-port
DB_NAME=your-db-name
```
