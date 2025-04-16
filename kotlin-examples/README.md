## Getting Started with Kafka and Flink with Kotlin

Example Kafka and Flink applications using Kotlin.

### Applications

- [Kafka JSON Clients](./kafka-json-clients/)

  - Kafka producer and consumer applications with JSON values.

  ```
   👉 With Gradle (Dev Mode)
   ./gradlew run --args="producer"
   ./gradlew run --args="consumer"

   👉 Build Shadow (Fat) JAR:
   ./gradlew shadowJar

   Resulting JAR:
   build/libs/kafka-json-clients-1.0.jar

   👉 Run the Fat JAR:
   java -jar build/libs/kafka-json-clients-1.0.jar producer
   java -jar build/libs/kafka-json-clients-1.0.jar consumer
  ```

- [Kafka AVRO Clients](./kafka-avro-clients/)

  - Kafka producer and consumer applications with AVRO values, integrated with the Confluent Schema Registry.

  ```
   👉 With Gradle (Dev Mode)
   ./gradlew run --args="producer"
   ./gradlew run --args="consumer"

   👉 Build Shadow (Fat) JAR:
   ./gradlew shadowJar

   Resulting JAR:
   build/libs/kafka-avro-clients-1.0.jar

   👉 Run the Fat JAR:
   java -jar build/libs/kafka-avro-clients-1.0.jar producer
   java -jar build/libs/kafka-avro-clients-1.0.jar consumer
  ```

### Resources

- Kafka Client
  - [Create a Kafka Client App for Kotlin for Use With Confluent Platform](https://docs.confluent.io/platform/current/clients/examples/kotlin.html)
  - [Kafka tutorial - Simple client API](https://github.com/aseigneurin/kafka-tutorial-simple-client)
  - [Kafka producer and consumer written with Kotlin](https://lankydan.dev/kafka-producer-and-consumer-written-with-kotlin)
  - [Interacting with Kafka with Kotlin Coroutines](https://nabeelvalley.co.za/blog/2023/11-11/interacting-with-kafka-using-kotlin/)
- Kafka Streams
  - [Getting Started With Kafka Streams](https://lucapette.me/writing/getting-started-with-kafka-streams/)
- Event Streaming/Microservices
  - [Apache Kafka Event Streaming Platform For Kotlin Developers](https://www.youtube.com/watch?v=Y-sqGKsnSHI)
  - [Applied Event Streaming With Apache Kafka, Kotlin, and Ktor](https://www.youtube.com/watch?v=6qxkawU0qKA)
  - [Event-Driven Microservices with Apache Kafka, Kotlin, and Ktor](https://www.youtube.com/watch?v=x9l_6E4jIQY)
    - [Learn how to build event-driven microservices with Apache Kafka, Kotlin, and Ktor](https://gamov.io/workshop/2021/03/30/ktor-kafka-2021.html)
