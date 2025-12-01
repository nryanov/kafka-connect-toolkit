plugins {
    id("java-test-fixtures")
}

dependencies {
    testFixturesImplementation(libs.confluent.schema.registry.client)
    testFixturesImplementation(libs.avro)
    testFixturesImplementation(libs.testcontainers.core)
}