plugins {
    kotlin("jvm") version "2.1.20"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
    id("com.github.johnrengelman.shadow") version "8.1.1"
    application
}

group = "me.jaehyeon"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.9.0")
    implementation("org.apache.kafka:kafka-streams:3.9.0")
    // AVRO
    implementation("org.apache.avro:avro:1.11.4")
    implementation("io.confluent:kafka-streams-avro-serde:7.9.0")
    // Json
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.0")
    // Logging
    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
    implementation("ch.qos.logback:logback-classic:1.5.13")
    // Test
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("me.jaehyeon.MainKt")
}

avro {
    setCreateSetters(false)
    setFieldVisibility("PRIVATE")
}

tasks.named("compileKotlin") {
    dependsOn("generateAvroJava")
}

sourceSets {
    named("main") {
        java.srcDirs("build/generated/avro/main")
        kotlin.srcDirs("src/main/kotlin")
    }
}

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    archiveBaseName.set("orders-stats-streams")
    archiveClassifier.set("")
    archiveVersion.set("1.0")
    mergeServiceFiles()
}

tasks.named("build") {
    dependsOn("shadowJar")
}

tasks.named<JavaExec>("run") {
    environment("BOOTSTRAP", "localhost:9092")
    environment("TOPIC", "orders-avro")
    environment("REGISTRY_URL", "http://localhost:8081")
}

tasks.test {
    useJUnitPlatform()
}
