plugins {
    id("java-test-fixtures")
}

dependencies {
    testFixturesImplementation(libs.testcontainers.core)
    testFixturesImplementation(libs.jdbi.core)
    testFixturesImplementation(libs.avro)
    testFixturesApi(testFixtures(project(":modules:fixtures:fixtures-kafka")))
    testFixturesApi(testFixtures(project(":modules:fixtures:fixtures-schema-registry")))
    testFixturesApi(testFixtures(project(":modules:fixtures:fixtures-postgres")))
    testFixturesApi(testFixtures(project(":modules:fixtures:fixtures-debezium")))
    // for connector api
    testFixturesImplementation(libs.testcontainers.debezium)
}