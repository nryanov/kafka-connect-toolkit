plugins {
    id("java-test-fixtures")
}

dependencies {
    testFixturesImplementation(libs.postgres.driver)
    testFixturesImplementation(libs.testcontainers.postgresql)
}