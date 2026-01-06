# 🏦 Midas: Real-Time Payment Processing Engine

![Java](https://img.shields.io/badge/Java-17-orange) ![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.0-green) ![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Event_Streaming-black) ![JPA](https://img.shields.io/badge/Spring_Data-JPA-blue)

**Midas** is a high-throughput, event-driven banking ledger system designed to process financial transactions in real-time. Built as part of the **JPMorgan Chase & Co. Software Engineering Virtual Experience**, this application demonstrates the integration of distributed messaging systems, relational databases, and RESTful microservices architecture.

## 🚀 Key Technical Achievements

* **Engineered Event-Driven Architecture:** Architected a robust **Spring Boot** application capable of consuming high-velocity transaction data from **Apache Kafka** topics (`trader-updates`), ensuring strictly ordered processing of financial messages.
* **Implemented ACID-Compliant Persistence:** Designed **Spring Data JPA** entities and repositories to persist User and Transaction data into an **H2 Database**, ensuring data integrity and atomic balance updates.
* **Orchestrated Microservice Communication:** Integrated an external **Incentives API** using `RestTemplate` to synchronously fetch and apply algorithmic rewards to eligible transactions, simulating a multi-service banking environment.
* **Developed RESTful Endpoints:** Exposed user financial data via a custom, secure **REST API Controller** running on a dedicated port (`33400`), allowing for external querying of user balances.
* **Optimized Error Handling:** Implemented validation logic to prevent overdrafts and handle idempotent transaction processing, ensuring the system remains resilient under load.

## 🛠️ Tech Stack

* **Language:** Java 17
* **Framework:** Spring Boot (Web, Data JPA, Kafka)
* **Messaging:** Apache Kafka (Zookeeper/KRaft)
* **Database:** H2 In-Memory Database (Hibernate ORM)
* **Build Tool:** Maven / Gradle
* **Testing:** JUnit 5, Spring Boot Test, EmbeddedKafka

## 🧩 System Architecture

The Midas Core system operates via the following data flow:

1.  **Ingestion:** A `KafkaProducer` publishes serialized JSON transaction objects to the `trader-updates` topic.
2.  **Consumption:** The `KafkaConsumer` listener intercepts the message, deserializes it, and initiates the transaction flow.
3.  **Validation & Enrichment:** The system queries the local database to validate sender funds. If valid, it makes an HTTP POST request to the **Incentives Microservice** to calculate reward logic.
4.  **Persistence:** Sender and Recipient balances are updated atomically in the database, and a record of the transaction (including the incentive) is archived.
5.  **Exposure:** A `BalanceController` listens for GET requests to provide real-time updates on user account standing.

## 💻 How to Run

### Prerequisites
* Java SDK 17+
* Apache Kafka (Running locally)
* External Incentives API JAR (Provided in `/services`)

### Execution Steps

1.  **Start Apache Kafka:**
    Ensure Zookeeper and Kafka Server are running on your local machine.

2.  **Launch Incentives API:**
    ```bash
    cd services
    java -jar transaction-incentive-api.jar
    ```

3.  **Run Midas Core:**
    ```bash
    ./mvnw spring-boot:run
    ```
    *The application will start on port `33400`.*

## 🧪 Testing

The project includes a comprehensive suite of integration tests (`TaskThreeTests`, `TaskFourTests`, `TaskFiveTests`) that utilize **EmbeddedKafka** to simulate a live production environment.

To run the full test suite:
```bash
./mvnw test

📜 Certification
Completed as part of the JPMorgan Chase & Co. Software Engineering Job Simulation.
