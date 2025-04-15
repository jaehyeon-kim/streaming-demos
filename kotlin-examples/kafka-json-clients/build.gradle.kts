plugins {
    kotlin("jvm") version "2.1.10"
    application
}

group = "me.jaehyeon"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.9.0")
    // JSON (using Jackson)
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")
    // Logging
    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
    implementation("ch.qos.logback:logback-classic:1.4.14")
    // Faker
    implementation("net.datafaker:datafaker:2.1.0")
    // Test
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("me.jaehyeon.MainKt")
}
