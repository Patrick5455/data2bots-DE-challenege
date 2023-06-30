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

2. Clone the project repository to your local machine.

3. Navigate to the root folder of the project, where the `docker-compose.yml` file is located.

4. Open a terminal and run the following command to build the Docker containers:

   ```bash
   docker-compose build

5. Once the containers are built, start the Airflow container by running:

   ```bash
   docker-compose up airflow-init
This will initialize the Airflow database and create the necessary tables.

6. After the initialization is complete, start the Airflow scheduler and webserver:

   ```bash
   docker-compose up
This will start the Airflow scheduler and webserver as daemon processes.

7. Access the Airflow UI by navigating to http://localhost:8080 in your web browser.

8. In the Airflow UI, you can monitor and manage the DAGs (Directed Acyclic Graphs), which represent the workflow of your ELT project. The DAG responsible for the ELT pipeline (***batch_pipeline_dag.py***) should be visible.

9. Click on the DAG to view its details and enable it if necessary. You can also configure the scheduling interval to match your requirements.

10. Once the DAG is enabled, it will run according to the specified schedule, executing the steps of the ELT pipeline.

## Production-Grade Project Features
The ELT project incorporates several production-grade features, including:

- Containerization: The project is containerized using Docker, providing a consistent and isolated environment across different systems.

- Job Orchestration: Airflow is used as the job orchestrator, providing a reliable and scalable framework for managing and scheduling the ELT pipeline.

- Version Control: The project code and configurations are stored in a Git repository, enabling version control and collaboration among team members.

- Modular Design: The project is structured into separate modules for different functionalities, such as staging, transformations, and dumping results. This promotes code reusability, maintainability, and ease of testing.

- Scalability: With Docker and Airflow, the project can be scaled horizontally by adding more workers to handle increased data volumes or processing requirements.

- Monitoring and Alerting: Airflow provides






