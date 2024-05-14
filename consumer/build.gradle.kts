plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Apache Kafka
    implementation ("org.apache.kafka:kafka-clients:3.7.0")
    implementation ("org.slf4j:slf4j-api:2.0.13")
    implementation ("org.slf4j:slf4j-simple:2.0.13")
}

tasks.test {
    useJUnitPlatform()
}