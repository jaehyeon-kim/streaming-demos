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
    maven("https://packages.confluent.io/maven")
}

dependencies {
    // Flink Core and APIs
    implementation("org.apache.flink:flink-streaming-java:1.20.1")
    implementation("org.apache.flink:flink-table-api-java:1.20.1")
    implementation("org.apache.flink:flink-table-api-java-bridge:1.20.1")
    implementation("org.apache.flink:flink-table-planner-loader:1.20.1")
    implementation("org.apache.flink:flink-table-runtime:1.20.1")
    implementation("org.apache.flink:flink-clients:1.20.1")
    implementation("org.apache.flink:flink-connector-base:1.20.1")
    // Flink Kafka and Avro
    implementation("org.apache.flink:flink-connector-kafka:3.4.0-1.20")
    implementation("org.apache.flink:flink-avro:1.20.1")
    implementation("org.apache.flink:flink-avro-confluent-registry:1.20.1")
    // Json
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.0")
    // Logging
    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
    implementation("ch.qos.logback:logback-classic:1.5.13")
    // Kotlin test
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("me.jaehyeon.MainKt")
    applicationDefaultJvmArgs =
        listOf(
            "--add-opens=java.base/java.util=ALL-UNNAMED",
        )
}

avro {
    setCreateSetters(true)
    setFieldVisibility("PUBLIC")
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
    archiveBaseName.set("orders-stats-flink")
    archiveClassifier.set("")
    archiveVersion.set("1.0")
    mergeServiceFiles()
}

tasks.named("build") {
    dependsOn("shadowJar")
}

tasks.named<JavaExec>("run") {
    environment("TO_SKIP_PRINT", "false")
    environment("BOOTSTRAP", "localhost:9092")
    environment("REGISTRY_URL", "http://localhost:8081")
}

tasks.test {
    useJUnitPlatform()
}
