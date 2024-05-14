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

    // OpenSearch Client
    implementation("org.opensearch.client:opensearch-rest-high-level-client:2.13.0")
    implementation("com.google.code.gson:gson:2.9.0")
}

tasks.test {
    useJUnitPlatform()
}