# UNIX_PROJECT

🚀 Automated Reddit Topic Classifier & MLOps Pipeline
Project Overview
This project implements a complete MLOps infrastructure designed to ingest, process, and classify social media content from Reddit. The system operates on a Batch Inference pattern, automatically fetching new data daily and predicting the subreddit category based on the post title using a trained Machine Learning model.

Key Features
Automated Data Ingestion: Python scripts fetching data from the Reddit API with robust error handling and duplicate prevention.

Modern Data Warehousing: Utilizes Snowflake as the central data store, leveraging the VARIANT column for efficient semi-structured (JSON) data handling and SQL Views for the "Silver" layer.

Orchestration with Airflow: Two distinct DAGs managed by Apache Airflow running in Docker Containers:

Initialization Pipeline: Sets up infrastructure, ingests historical data, and trains the base model.

Daily Inference Pipeline: Runs on a schedule to fetch new posts (last 24h) and generate predictions.

Machine Learning Pipeline: A scikit-learn pipeline performing text vectorization (TF-IDF) and classification (Random Forest). The system includes an Offline Evaluation Store to track real_label vs predicted_label directly in the database.

Architecture & Technologies
Infrastructure: Docker, Bash scripts
Orchestration: Apache Airflow 2.8+
Data Warehouse: Snowflake (SQLAlchemy, JSON support)
Language: Python (Pandas, Scikit-learn, etc.)
