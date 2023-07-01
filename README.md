# ELT Project

The ELT project is a batch processing pipeline designed to load data from a data lake into a PostgreSQL data warehouse. It performs transformations and executes analytics queries using SQL and psycopg2 to generate insights, and stores the results in the analytics schema of the data warehouse. Additionally, the transformed data is dumped back into the data lake.

## Project Overview

The ELT project utilizes Airflow as the job orchestrator to manage scheduling and resource allocation. It is designed to run every midnight, ensuring regular updates and data processing. The pipeline consists of the following components:

1. **Staging**: Data is extracted from the data lake (S3) and loaded into the staging schema of the PostgreSQL data warehouse. The staging tables include "reviews," "orders," and "shipment_deliveries."

2. **Transformations**: Using SQL and psycopg2, analytics queries are executed on the staging data to perform transformations and generate insights. The result of the transformations is saved as tables in the analytics schema of the data warehouse. Initially, DBT was considered for transformations, but due to permission constraints, SQL and Python with psycopg2 were utilized instead.

3. **Dumping Results**: The transformed data is dumped back into the data lake (S3) for further analysis and storage.

## Running the Project

To run the ELT project, follow the steps below:

1. Make sure Docker and Docker Compose are installed on your machine.
2. create a .env file with the following configs:
```
#DB Config
DB_HOST=34.89.230.185
DB_PORT=5432
DB_NAME=d2b_accessment
STAGING_DB_SCHEMA=patrojun6040_staging
ANALYTICS_DB_SCHEMA=patrojun6040_analytics
DB_USER={YOUR_USER}
DB_PASSWORD={YOUR_PASSWORD}
 
NB: dno't include the {} when adding your user and password
 
# Meta-Database
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow Core
AIRFLOW__CORE__FERNET_KEY=UKMzEm3yIuFYEq1y3-2FxPNWSVwRASpahmQ9kQfEr8E=
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW_UID=0

# Backend DB
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS=False

# Airflow Init
_AIRFLOW_DB_UPGRADE=True
_AIRFLOW_WWW_USER_CREATE=True
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```
4. Clone the project repository to your local machine.
5. Navigate to the root folder of the project, where the `docker-compose.yml` file is located.
6. Open a terminal and run the following command to build the Docker containers:
   ```bash
   docker-compose build
   ```
7. Once the containers are built, start the Airflow container by running:
   ```bash
   docker-compose up
   ```
   - This will start the Airflow scheduler and webserver as daemon processes.


8. Access the Airflow UI by navigating to http://localhost:8080 in your web browser.

9. In the Airflow UI, you can monitor and manage the DAGs (Directed Acyclic Graphs), which represent the workflow of your ELT project. The DAG responsible for the ELT pipeline (***batch_pipeline_dag.py***) should be visible.

10. Click on the DAG to view its details and enable it if necessary. You can also configure the scheduling interval to match your requirements.

11. Once the DAG is enabled, it will run according to the specified schedule, executing the steps of the ELT pipeline.

**Below is an image of a successful run**

![Successful DAG RUN](./Successful_Dag_Run.png)

### Simple Run

For a simple run of the project, after setting your .env config file at the project root, 

At the root of *__data2bots_elt/dags__* folder, 

run

   ```bash
   python3 main.py
   ```
## Production-Grade Project Features
The ELT project incorporates several production-grade features, including:

- **Containerization**: 
  - The project is containerized using Docker, providing a consistent and isolated environment across different systems.

- **Job Orchestration**: 
  - Airflow is used as the job orchestrator, providing a reliable and scalable framework for managing and scheduling the ELT pipeline.

- **Version Control**: 
  - The project code and configurations are stored in a Git repository, enabling version control and collaboration among team members.

- **Modular Design**: 
  - The project is structured into separate modules for different functionalities, such as staging, transformations, and dumping results. This promotes code reusability, maintainability, and ease of testing.

- **Scalability**: 
  - With Docker and Airflow, the project can be scaled horizontally by adding more workers to handle increased data volumes or processing requirements.
  
## Limitations and improvements
- Due to limited time constraints, I could not provide a monitoring and alerting feature
  - I could not complete the slack/email notification task responsible for notifying respective group/person(s) of a successful DAG run or a failed dag run 
- Due to DB permission issues, 
  - I could not have DBT connect to the postgres DB to use DBT as a transformation tool within the postgres DWH. Albeit, I still maintained the DBT setup as well as the transformation SQL queries in it
- As an improvement
  - I would love to add an analytics dashboard that reads from the `patrojun6040_analytics` schema with some nice and interesting data visualization 
  - Also, as an improvement on the production grade, I would manage airflow using a tool like astronomer or GCP managed Airflow, this way, I would be able to demonstrate my IAC skill and experience with Terraform. 

## Suggestions and Contributions:
- Shoot me an email [patrickojunde@gmail.com](mailto:patrickojunde@gmail.com)



