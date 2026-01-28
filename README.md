# Huberman Lab YouTube Data Pipeline (Azure Functions)
## Project Overview
This project implements a cloud-based, serverless data pipeline to ingest and process YouTube podcast data using Microsoft Azure.
The pipeline pulls video and comment data from the YouTube Data API, processes it through structured data layers, and enriches the data using Azure OpenAI.
## Architecture
YouTube Data API  
→ Azure Functions (HTTP-triggered ingestion)  
→ Azure Blob Storage (Bronze / Silver / Gold layers)  
→ Azure OpenAI (GPT-4o enrichment)
## Technologies Used
- Azure Functions (Python)
- Azure Blob Storage
- Azure OpenAI (GPT-4o)
- YouTube Data API
- Python
- REST APIs
## Data Pipeline Design
- **Bronze Layer**: Raw YouTube API responses stored as JSON
- **Silver Layer**: Cleaned and normalized datasets
- **Gold Layer**: AI-enriched datasets with sentiment and emotion analysis
## Key Features
- Serverless ingestion using Azure Functions
- Cloud storage using Azure Blob containers
- AI-powered enrichment using Azure OpenAI
- Structured, production-style data pipeline
## Outputs
- comments_with_sentiment.json
- videos_with_sentiment.json
- kpis.json
## Notes
This project focuses on data ingestion, processing, and AI enrichment.
