import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.JavaExec
import org.gradle.kotlin.dsl.withType

plugins {
    kotlin("jvm") version "2.2.21"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "me.jaehyeon"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven")
}

val flinkVersion = "1.20.1"
val log4jVersion = "2.17.1"
val avroVersion = "1.11.3"

configurations.all {
    resolutionStrategy {
        force("org.apache.avro:avro:$avroVersion")
    }
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
    exclude(group = "org.slf4j", module = "slf4j-reload4j")
    exclude(group = "log4j", module = "log4j")
}

val localRunClasspath by configurations.creating {
    extendsFrom(configurations.implementation.get(), configurations.compileOnly.get(), configurations.runtimeOnly.get())
}

dependencies {
    // Flink Dependencies
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")
    compileOnly("org.apache.flink:flink-connector-base:$flinkVersion")
    compileOnly("org.apache.flink:flink-connector-files:$flinkVersion")
    // 'testImplementation' makes Flink available for test source compilation and execution.
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-clients:$flinkVersion")
    testImplementation("org.apache.flink:flink-connector-base:$flinkVersion")
    testImplementation("org.apache.flink:flink-connector-files:$flinkVersion")
    // Kafka and Avro
    implementation("org.apache.kafka:kafka-clients:3.9.1")
    implementation("org.apache.flink:flink-connector-kafka:3.4.0-1.20")
    implementation("org.apache.flink:flink-avro:$flinkVersion")
    implementation("org.apache.flink:flink-avro-confluent-registry:$flinkVersion")
    implementation("org.apache.avro:avro:$avroVersion")
    // Matrix Algebra
    implementation("org.apache.commons:commons-math3:3.6.1")
    // Redis Client (Jedis)
    implementation("redis.clients:jedis:5.1.5")
    // JSON Serialization
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.15.2")
    // Logging
    runtimeOnly("org.apache.logging.log4j:log4j-api:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-core:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.14.1")
}

kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set("me.jaehyeon.MainKt")
}

tasks.withType<ShadowJar> {
    archiveBaseName.set(rootProject.name)
    archiveClassifier.set("")
    archiveVersion.set("1.0")

    mergeServiceFiles()

    // Relocate Jackson to avoid conflicts with Flink's internal Jackson version
    relocate("com.fasterxml.jackson", "io.factorhouse.shaded.jackson")

    dependencies {
        exclude(dependency("org.apache.logging.log4j:.*"))
        exclude(dependency("org.slf4j:.*"))
    }
}

tasks.named("build") {
    dependsOn("shadowJar")
}

tasks.named<JavaExec>("run") {
    val avroJar =
        localRunClasspath.files.find { it.name.contains("avro-$avroVersion") }
            ?: throw GradleException("Avro $avroVersion jar not found in classpath!")
    classpath = files(avroJar) + localRunClasspath + sourceSets.main.get().output

    environment("BOOTSTRAP", "localhost:9092")
    environment("REGISTRY_URL", "http://localhost:8081")
    environment("REDIS_HOST", "localhost")

    val resourceFile = project.file("src/main/resources/training_log.csv")
    environment("EVENT_LOG", "file://${resourceFile.absolutePath}")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
