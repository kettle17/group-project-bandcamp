# 🎵🎧 Tracktion 🎧🎵
Tracktion is a service that will use the Bandcamp API to monitor artist and song analytics using a cloud-based data pipeline and dashboard to monitor this information, send reports and alerts to musicians and labels, and enable data-driven decisions via interactive visualisations.

## 🚰 ETL Pipeline
The ETL pipeline will ingest BandCamp sales data, transform it into structured data, and load it into a cloud-hosted RDS database for storage and analysis. The pipeline will run on a containerised infrastructure using Docker and Terraform.

## 📈 Visualisation
The data is visualised using Streamlit, a lightweight web app framework chosen for its cost effectiveness and customisation capabilities. Developers can adapt the dashboards to their own needs, offering flexibility in how insights are displayed.

## 📍 Folder Navigation
- `documentation/`: ERD, architecture diagram, WBS breakdown, dashboard wireframe, and user stories.
- `pipeline/`: ETL scripts and related tests.
- `terraform/`: Infrastructure-as-code files, including Docker configuration for the ETL pipeline.


## 🤐 Environment Variable Structure
The following are the variables required in the .env file with their placeholders:
```

```