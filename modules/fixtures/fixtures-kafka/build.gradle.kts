plugins {
    id("java-test-fixtures")
}

dependencies {
    testFixturesImplementation(libs.testcontainers.redpanda)
    testFixturesApi(libs.kafka.clients)
}