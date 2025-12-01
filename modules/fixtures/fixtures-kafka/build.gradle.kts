plugins {
    id("java-test-fixtures")
}

dependencies {
    testFixturesImplementation(libs.testcontainers.redpanda)
    testFixturesApi(libs.kafka.clients)
    testFixturesApi(libs.confluent.avro.converter)
    testFixturesApi(libs.avro)

    testFixturesImplementation(testFixtures(project(":modules:fixtures:fixtures-schema-registry")))
}