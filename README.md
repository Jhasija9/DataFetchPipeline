# Distributed Streaming Pipeline with Kafka, Airflow, Spark, and Cassandra

## Overview
This project demonstrates a distributed data pipeline for real-time data ingestion, processing, and storage. The system fetches data from an external API, streams it through Apache Kafka, processes it using Apache Spark, and stores the results in a Cassandra database. The orchestration and scheduling are managed by Apache Airflow, and the entire architecture is containerized using Docker.

## Architecture
The system architecture includes the following components:

1. **API Fetching**: Data is retrieved from an external API.
2. **Apache Airflow**: Orchestrates the data flow and scheduling tasks.
3. **Apache Kafka**: Acts as the message broker, streaming data between components.
4. **Control Center and Schema Registry**: Monitors and manages Kafka topics.
5. **Apache Spark**: Processes the streamed data in real-time.
6. **Cassandra Database**: Stores the processed data.
7. **Docker**: Containerizes all the services to ensure portability and scalability.

![image](https://github.com/user-attachments/assets/82b45af3-2849-4149-8e12-ac25dc59949f)

## Features
- Real-time data fetching from an external API.
- Stream processing with Apache Kafka and Apache Spark.
- Distributed storage using Cassandra.
- Fully containerized with Docker Compose.
- Orchestrated workflows using Apache Airflow.

## Repository Structure
```
.
├── dags/
│   └── kafka_stream.py  # Airflow DAG for orchestrating the pipeline
├── script/
│   └── entrypoint.sh    # Entrypoint script for the containers
├── docker-compose.yml      # Docker Compose configuration
├── requirements.txt       # Python dependencies
├── spark_stream.py        # Spark job for processing Kafka streams
└── README.md              # Project documentation
```

## Prerequisites
- Docker and Docker Compose installed
- Python 3.9+
- Apache Kafka, Zookeeper, and Cassandra (set up in Docker Compose)

## Setup and Installation

1. **Clone the Repository**
   ```bash
   git clone https://github.com/your-repo-name.git
   cd your-repo-name
   ```

2. **Install Dependencies**
   Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Docker Containers**
   Start all services (Kafka, Cassandra, Airflow, Spark) using Docker Compose:
   ```bash
   docker-compose up -d
   ```

4. **Airflow Configuration**
   - Access the Airflow UI at `http://localhost:8080`.
   - Trigger the `kafka_stream` DAG to start the pipeline.

5. **Monitor Kafka**
   Use Kafka Control Center or CLI tools to monitor the topics.

## Usage

1. **Run the Pipeline**
   Trigger the Airflow DAG, which:
   - Fetches data from the API.
   - Sends the data to Kafka.
   - Processes the Kafka streams using Spark.
   - Stores the processed data into Cassandra.

2. **Verify Data in Cassandra**
   Log into the Cassandra container:
   ```bash
   docker exec -it cassandra cqlsh
   ```
   Query the data:
   ```sql
   SELECT * FROM spark_streams.created_users;
   ```

## Key Scripts

1. **kafka_stream.py**
   - Defines the Airflow DAG.
   - Handles API data fetching and Kafka streaming.

2. **spark_stream.py**
   - Processes streamed data using Spark.
   - Writes the processed data to Cassandra.

3. **docker-compose.yml**
   - Defines all container services (Kafka, Zookeeper, Spark, Airflow, Cassandra).

4. **entrypoint.sh**
   - Configures and starts services within containers.

## Example Output
You can verify the processed data in Cassandra:
```sql
SELECT first_name, last_name FROM spark_streams.created_users;
```
![image](https://github.com/user-attachments/assets/17d9cfc9-3a04-4eba-becd-94365999c9f0)

![image](https://github.com/user-attachments/assets/4164b29c-9973-418d-b4bc-c367375d8a18)



## Challenges and Solutions
- **Real-time Data Processing**: Leveraged Kafka’s scalability and Spark’s streaming capabilities.
- **Distributed Storage**: Used Cassandra for fault-tolerant and scalable storage.
- **Orchestration**: Airflow streamlined task scheduling and monitoring.

## Future Enhancements
- Add more data sources for ingestion.
- Implement data quality checks in the pipeline.
- Enhance monitoring and logging.

## Conclusion
This project showcases a robust real-time data pipeline leveraging modern distributed systems. It highlights the seamless integration of Apache Kafka, Spark, Cassandra, and Airflow to achieve scalable, reliable, and efficient data processing.

