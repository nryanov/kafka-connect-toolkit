plugins {
    id("java-test-fixtures")
}

dependencies {
    testFixturesImplementation(libs.testcontainers.core)
    testFixturesImplementation(libs.jdbi.core)
    testFixturesImplementation(libs.avro)
    testFixturesImplementation(testFixtures(project(":modules:fixtures:fixtures-jar")))
    testFixturesImplementation(testFixtures(project(":modules:fixtures:fixtures-kafka")))
    testFixturesImplementation(testFixtures(project(":modules:fixtures:fixtures-schema-registry")))
    testFixturesImplementation(testFixtures(project(":modules:fixtures:fixtures-postgres")))
    testFixturesImplementation(testFixtures(project(":modules:fixtures:fixtures-debezium")))
    // for connector api
    testFixturesImplementation(libs.testcontainers.debezium)
}