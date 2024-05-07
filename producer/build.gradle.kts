plugins {
    id("java")
}

group = "org.example"

repositories {
    mavenCentral()
}

dependencies {
    // Apache Kafka
    implementation ("org.apache.kafka:kafka-clients:3.7.0")
    implementation ("org.slf4j:slf4j-api:2.0.13")
    implementation ("org.slf4j:slf4j-simple:2.0.13")

    // https://mvnrepository.com/artifact/com.launchdarkly/okhttp-eventsource
    implementation("com.launchdarkly:okhttp-eventsource:4.1.1")
}

tasks.test {
    useJUnitPlatform()
}