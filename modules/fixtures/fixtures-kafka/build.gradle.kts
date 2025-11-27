plugins {
    id("java-test-fixtures")
}

dependencies {
    testFixturesImplementation(libs.testcontainers.kafka)
    testFixturesApi(libs.kafka.clients)
}