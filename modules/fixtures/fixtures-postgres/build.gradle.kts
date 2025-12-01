plugins {
    id("java-test-fixtures")
}

dependencies {
    testFixturesImplementation(libs.postgres.driver)
    testFixturesImplementation(libs.jdbi.core)
    testFixturesImplementation(libs.agroal)
    testFixturesImplementation(libs.testcontainers.postgresql)
}