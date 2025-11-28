plugins {
    id("java-test-fixtures")
}

dependencies {
    testFixturesApi(libs.kafka.connect)
    testFixturesApi(libs.debezium.api)
    testFixturesImplementation(libs.testcontainers.debezium)
}