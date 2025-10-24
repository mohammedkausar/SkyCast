# 🌤️ **SkyCast Weather Data Pipeline**

## **Overview**

SkyCast is a serverless, event-driven **Data Engineering pipeline** built on AWS that automates the process of fetching,
transforming, and loading weather data into a PostgreSQL RDS database.

The system leverages **AWS Lambda**, **S3**, **EventBridge**, and **RDS** to create a cost-effective and scalable data
flow for weather analytics.

---

## **🔁 Project Architecture**

### **Data Flow Summary**

1. **Extract & Transform Lambda (`lambda_extract_transform`)**
    - Fetches city-wise weather data from **OpenWeather API**.
    - Cleans, optimizes, and converts the data into Parquet format.
    - Stores processed files in **S3 bucket** under `staging/` prefix.
    - Triggered **daily at 2:00 AM** by an **EventBridge Scheduler**.

2. **Load Lambda (`lambda_load`)**
    - Automatically triggered when a **new file lands in the S3 `staging/` folder**.
    - Reads the transformed data and loads it into **PostgreSQL (RDS)** using `psycopg2`.
    - Reads the data from staging table and loads into **PostgreSQL (RDS)** using `psycopg2` **star schema** based tables.
   
---
## **DB Tables**

### Staging
    raw_weather - De-normalised table with raw data 

### Data modelled tables - *STAR SCHEMA*

#### Fact Tables
        fact_weather - quantitative metrics from raw_weather for analysis purpose

#### Dimension Tables
        dim_location
        dim_weather
                - descriptive metrics from raw_weather
---

## **🧩 Architecture Diagram**
<img style="background-color:seagreen; padding:2px; border-radius:8px" src="assets/skycast_architechture.jpg" alt="SkyCast ETL Architecture Diagram" width="1000"/>

---

## **⚙️ Project Structure**
```
skycast/
│
├── config/
│ ├── config.json # Area where all config data for the project is maintained!
├── src/
│ ├── ETL/
│ │ ├── extract.py # Extracts data from OpenWeather API
│ │ ├── transform.py # Cleans and optimizes dataset
│ │ ├── stage.py # Uploads data to S3 at various stages
│ │ ├── load.py # Loads transformed data into PostgreSQL
│ │
│ ├── utils/
│ │ ├── db_connect.py # Fetch DB configs from environment variables
│ │ ├── ddl_actions.py # Table creation and maintenance scripts
│ │ ├── data_cleaner.py # Clean data and handle nulls
│ ├── convert_dtype.py # Convert the datatype from source to python compatible
│ ├── s3_ops.py # A util method to handle all aws s3 operations
│
├── config/
│ └── config.json # Contains API keys, S3 paths, DB credentials, etc.
│
├── lambda_extract_transform.py # Lambda for extraction and transformation
├── lambda_load.py # Lambda for loading data to RDS
├── lambda_star_schema_load.py # Lambda for loading fact and dim tables in RDS
├── main.py # Local testing and manual pipeline execution
├── requirements.txt # Python dependencies
└── README.md # Project documentation
```
---

## **🚀 AWS Lambda Deployment & Dependencies**

### **Lambda Functions**

#### **1. `lambda_extract_transform`**

- Handles **data extraction and transformation**.
- Fetches data from **OpenWeather API**, cleans it, and stores it as `weather_data_<cdc>.parquet` in **S3/staging/**.
- Triggered by an **AWS EventBridge Scheduler** every day at **2:00 AM**.

#### **2. `lambda_load`**

- Automatically triggered when a new file is uploaded to the **S3 `staging/` folder** of the `skycast-weather-report`
  bucket.
- Loads data into **PostgreSQL RDS** using `psycopg2`.
- Ensures schema consistency and handles incremental inserts.

#### **3. `lambda_load_star_schema`**

- Automatically triggered after a successful execution of the **`lambda_load`** function via a **Step Function**.
- Runs a stored procedure in the **Skycast RDS** database.
- The stored procedure loads data into **star schema tables** and updates the **`weather_cdc`** table to maintain **Change Data Capture (CDC)**.
- Ensures **data integrity**, **incremental updates**, and **schema consistency** across fact and dimension tables.


---

### **Python Runtime & Layers**

- **Python Version:** `3.13`
- **Shared Layer:** Common dependencies like `pandas`, `boto3`, `pyarrow`.
- **Custom Layer:**
    - Contains **`psycopg2`** compiled on **Amazon Linux 2023 AMI EC2**.
    - Attached to the `lambda_load` function using its **ARN reference**.

---

### **CI/CD Workflow (GitHub Actions)**

- Pushing to the *main* branch triggers **two GitHub Actions:**
    1. **Build & Package:**  
       Zips Lambda code with dependencies.
    2. **Deploy:**  
       Uploads packages to AWS Lambda (`extract_transform` and `load` and `load_star_schema`) automatically.

---

## **🔐 Environment Variables**

| Variable Name | Description                         |
|---------------|-------------------------------------|
| `DB_HOST`     | PostgreSQL host endpoint            |
| `DB_PORT`     | Database port number (default 5432) |
| `DB_NAME`     | Database name                       |
| `DB_USER`     | Username for RDS                    |
| `DB_PASSWORD` | Password for RDS                    |
| `OPENWEATHER_API_KEY`     | OpenWeather API key                 |


---

## **🧠 Key Features**

- **Serverless Architecture:** Fully managed AWS resources — no servers to maintain.
- **Event-Driven Triggers:** EventBridge for scheduling, S3 event for load automation.
- **Data Optimization:** Transformed data stored in efficient Parquet format.
- **Scalable Design:** Each stage is decoupled and independently triggerable.
- **Automated CI/CD:** Seamless Lambda updates through GitHub Actions.

---

## **🧰 Technologies Used**

| Category      | Tools/Services                                       |
|---------------|------------------------------------------------------|
| Cloud         | AWS Lambda, S3, RDS, EventBridge                     |
| Database      | PostgreSQL                                           |
| Language      | Python 3.13                                          |
| Packages      | `pandas`, `boto3`, `requests`, `psycopg2`, `pyarrow` |
| CI/CD         | GitHub Actions                                       |

---

## **📆 Future Enhancements**

- Add CloudWatch metrics dashboard for pipeline performance.
- Include S3 object versioning for data recovery.
- Integrate AWS Glue for schema cataloging.
- Extend API coverage for multiple weather parameters.

---

## **👨‍💻 Author**

**Mohammed Kawuser**  
*Data Engineer | Front-End Developer | Python | Postgres SQL | AWS & Cloud Data Pipelines*

